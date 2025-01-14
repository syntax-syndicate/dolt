// Copyright 2024 Dolthub, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package commands

import (
	"context"
	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/gen/fb/serial"
	"github.com/dolthub/dolt/go/libraries/doltcore/doltdb"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/tree"
	"github.com/dolthub/dolt/go/store/types"
	"os"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	_ "bazil.org/fuse/fs/fstestutil"
)

type mountOpts struct{}

var mountDocs = cli.CommandDocumentationContent{
	ShortDesc: `Mount Dolt as a file system`,
	LongDesc:  `Mount Dolt as a file system`,
	Synopsis: []string{
		`[{{.LessThan}}mountpoint{{.GreaterThan}}]`,
	},
}

type MountCmd struct{}

// Name returns the name of the Dolt cli command. This is what is used on the command line to invoke the command
func (cmd MountCmd) Name() string {
	return "mount"
}

// Description returns a description of the command
func (cmd MountCmd) Description() string {
	return `Mount Dolt as a file system`
}

func (cmd MountCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(showDocs, ap)
}

func (cmd MountCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 1)
	return ap
}

func (cmd MountCmd) RequiresRepo() bool {
	return true
}

func parseMountArgs(apr *argparser.ArgParseResults) (*mountOpts, error) {
	return &mountOpts{}, nil
}

func (cmd MountCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, mountDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)

	_, err := parseMountArgs(apr)
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	mountpoint := apr.Arg(0)

	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("dolt"),
		fuse.Subtype("doltfs"),
	)

	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	defer c.Close()

	err = fs.Serve(c, FS{db: dEnv.DoltDB})
	if err != nil {
		return HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	return 0
}

// TODO: An extra layer on top for selecting the db
// TODO: should we be getting the root set for a branch?
// Different handlers for different message types, which specify different paths.
// Examples:
// Top level can be:
// - head / working / staged
// - a branch
// - a tag
// - an address
// - a remote
// - a fully qualified ref
// - root? for the root store? Kinda redundant
// /workingSets/heads/main/ is a WorkingSet
// /workingSets/heads/main/working is a RootValue
// /refs/heads/main/ is a rootvalue
// /addresses/rt9gl00583v5ulof6qkhun355q6kcpbq is whatever the address resolves to.
// /working, /staged, and /head give you currently checked-out branch
/*
1)       { key: refs/heads/main ref: #h673mspupgcuisrbomql5ve84oeci9ae - commit -> root value
2)       { key: refs/heads/otherBranch ref: #h673mspupgcuisrbomql5ve84oeci9ae - commit
3)       { key: refs/internal/create ref: #h673mspupgcuisrbomql5ve84oeci9ae - commit
4)       { key: refs/remotes/origin/main ref: #h673mspupgcuisrbomql5ve84oeci9ae - commit
5)       { key: refs/tags/aTag ref: #solo3k07o2dc3u4veq0nhklc9il24huk - tag -> commit
6)       { key: workingSets/heads/main ref: #vqthnij64k14fbmgppschunnk5vi4v2b - working set
7)       { key: workingSets/heads/otherBranch ref: #578h6hjd4h0ovp9i1n4hcuts1d2ujp52 - working set
*/

// FS implements the hello world file system.
type FS struct {
	dEnv *env.DoltEnv
	db   *doltdb.DoltDB
}

func (f FS) Root() (fs.Node, error) {
	return RootDirectory{
		dEnv: f.dEnv,
		db:   f.db,
	}, nil
}

type Directory interface {
	fs.Node
	fs.NodeStringLookuper
}

type ListableDirectory interface {
	Directory
	fs.HandleReadDirAller
}

type File interface {
	fs.Node
	fs.HandleReadAller
}

type BaseDirectory struct{}

var _ fs.Node = BaseDirectory{}

func (BaseDirectory) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0o555
	return nil
}

type RootDirectory struct {
	BaseDirectory
	dEnv *env.DoltEnv
	db   *doltdb.DoltDB
}

func (d RootDirectory) Lookup(ctx context.Context, name string) (fs.Node, error) {
	switch name {
	case "head", "HEAD":
		headRoot, err := d.dEnv.HeadRoot(ctx)
		if err != nil {
			return nil, err
		}
		return RootValueDirectory{rootValue: headRoot}, nil
	case "working", "WORKING":
		roots, err := d.dEnv.Roots(ctx)
		if err != nil {
			return nil, err
		}
		return RootValueDirectory{rootValue: roots.Working}, nil
	case "staged", "STAGED":
		roots, err := d.dEnv.Roots(ctx)
		if err != nil {
			return nil, err
		}
		return RootValueDirectory{rootValue: roots.Staged}, nil
	}

	// is it a branch?
	{
		branches, err := d.dEnv.GetBranches()
		if err != nil {
			return nil, err
		}

		branch, hasBranch := branches.Get(name)
		if hasBranch {
			path := branch.Merge.Ref.GetPath()
			refHash, err := d.db.GetHashForRefStr(ctx, path)
			if err != nil {
				return nil, err
			}
			return lookupHash(ctx, d.db, *refHash)
		}
	}

	// is it a tag?
	{
		tags, err := d.db.GetTagsWithHashes(ctx)
		if err != nil {
			return nil, err
		}

		for _, tag := range tags {
			if tag.Tag.Name == name {
				return commitDirectory{commit: tag.Tag.Commit}, nil
			}
		}
	}

	// is it a remote?
	{
		// or use GetRemotesWithHashes?
		remotes, err := d.dEnv.GetRemotes()
		if err != nil {
			return nil, err
		}
		_, hasRemote := remotes.Get(name)
		if hasRemote {
			// TODO: How to resolve a remote to a list of refs?
		}
	}

	// is it an address?
	if hashRegex.MatchString(name) {
		refHash, err := parseHashString(name)
		if err != nil {
			return nil, err
		}
		return lookupHash(ctx, d.db, refHash)
	}

	// when do we check when a ref spec is complete? Now when we add to it? Or when we do a lookup on it?
	// probably noe.
	return lookupRefSpec(ctx, d.db, name)
}

type refSpecDirectory struct {
	BaseDirectory
	segments []string
}

func lookupHash(ctx context.Context, db *doltdb.DoltDB, h hash.Hash) (fs.Node, error) {
	value, err := db.ValueReadWriter().ReadValue(ctx, h)
	if err != nil {
		return nil, err
	}
	switch v := value.(type) {
	case types.SerialMessage:
		node, fileId, err := tree.NodeFromBytes(v)
		switch fileId {
		case serial.BlobFileID:
			return &Blob{
				ns:   db.NodeStore(),
				node: node,
			}, err
		}
	}
	return nil, syscall.ENOENT
}

type RootValueDirectory struct {
	BaseDirectory
	rootValue doltdb.RootValue
}

func lookupRefSpec(ctx context.Context, db *doltdb.DoltDB, refSpec string) (fs.Node, error) {
	refHash, err := db.GetHashForRefStr(ctx, refSpec)
	if err != nil {
		return nil, err
	}
	{
		return lookupHash(ctx, db, *refHash)
	}

	// Is this function unnecessary?
	datas := doltdb.HackDatasDatabaseFromDoltDB(db)
	datasets, err := datas.DatasetsWithPrefix(ctx, refSpec+"/")
	if err != nil {
		return nil, err
	}
	if len(datasets) == 0 {
		return nil, syscall.ENOENT
	}

	// if the segments resolve to a full ref spec, return it.
	// if there's no ref spec it can resolve to, return an error.
	return partialRefSpecDirectory{db: db, refSpec: refSpec}, nil
}

type partialRefSpecDirectory struct {
	BaseDirectory
	db      *doltdb.DoltDB
	refSpec string
}

func (d partialRefSpecDirectory) Lookup(ctx context.Context, name string) (fs.Node, error) {
	return lookupRefSpec(ctx, d.db, d.refSpec+name)
}

type AddressesDirectory struct {
	BaseDirectory
	db *doltdb.DoltDB
}

var _ Directory = AddressesDirectory{}

func (d AddressesDirectory) Lookup(ctx context.Context, specRef string) (fs.Node, error) {
	// if it's a correct hash, read the node as a blob.
	if !hashRegex.MatchString(specRef) {
		return nil, syscall.ENOENT
	}
	refHash, err := parseHashString(specRef)
	if err != nil {
		return nil, err
	}

	return lookupHash(ctx, d.db, refHash)
}

type commitDirectory struct {
	BaseDirectory
	db     *doltdb.DoltDB
	commit *doltdb.Commit
}

func (c commitDirectory) Lookup(ctx context.Context, name string) (fs.Node, error) {
	//TODO implement me
	panic("implement me")
}

var _ Directory = commitDirectory{}

type IndexDir struct {
	BaseDirectory
	db   *doltdb.DoltDB
	keys []interface{}
}

func (i IndexDir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	//TODO implement me
	panic("implement me")
}

var _ Directory = IndexDir{}

/*
var dirDirs = []fuse.Dirent{
	{Inode: 2, Name: "hello", Type: fuse.DT_File},
}

func (Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	return dirDirs, nil
}*/

type Blob struct {
	ns   tree.NodeStore
	node tree.Node
}

var _ File = Blob{}

func (b Blob) Attr(ctx context.Context, a *fuse.Attr) (err error) {
	a.Mode = 0o444
	// TODO: Report size just from the tree
	size := uint64(0)
	err = tree.WalkNodes(ctx, b.node, b.ns, func(ctx context.Context, n tree.Node) error {
		if n.IsLeaf() {
			size += uint64(len(n.GetValue(0)))
		}
		return nil
	})
	a.Size = size
	return err
}

func (b Blob) ReadAll(ctx context.Context) (result []byte, err error) {
	err = tree.WalkNodes(ctx, b.node, b.ns, func(ctx context.Context, n tree.Node) error {
		if n.IsLeaf() {
			result = append(result, n.GetValue(0)...)
		}
		return nil
	})
	return result, err
}
