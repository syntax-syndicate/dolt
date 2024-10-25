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

package ci

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/env/actions/dolt_ci"
	"github.com/fatih/color"
	"os"
	"path/filepath"

	"github.com/dolthub/dolt/go/cmd/dolt/cli"
	"github.com/dolthub/dolt/go/cmd/dolt/commands"
	"github.com/dolthub/dolt/go/cmd/dolt/errhand"
	"github.com/dolthub/dolt/go/libraries/doltcore/env"
	"github.com/dolthub/dolt/go/libraries/utils/argparser"
)

var importDocs = cli.CommandDocumentationContent{
	ShortDesc: "Import a Dolt continuous integration workflow file into the database",
	LongDesc:  "Import a Dolt continuous integration workflow file into the database",
	Synopsis: []string{
		"{{.LessThan}}file{{.GreaterThan}}",
	},
}

type ImportCmd struct{}

// Name implements cli.Command.
func (cmd ImportCmd) Name() string {
	return "import"
}

// Description implements cli.Command.
func (cmd ImportCmd) Description() string {
	return importDocs.ShortDesc
}

// RequiresRepo implements cli.Command.
func (cmd ImportCmd) RequiresRepo() bool {
	return true
}

// Docs implements cli.Command.
func (cmd ImportCmd) Docs() *cli.CommandDocumentation {
	ap := cmd.ArgParser()
	return cli.NewCommandDocumentation(importDocs, ap)
}

// Hidden should return true if this command should be hidden from the help text
func (cmd ImportCmd) Hidden() bool {
	return true
}

// ArgParser implements cli.Command.
func (cmd ImportCmd) ArgParser() *argparser.ArgParser {
	ap := argparser.NewArgParserWithMaxArgs(cmd.Name(), 1)
	return ap
}

// Exec implements cli.Command.
func (cmd ImportCmd) Exec(ctx context.Context, commandStr string, args []string, dEnv *env.DoltEnv, cliCtx cli.CliContext) int {
	ap := cmd.ArgParser()
	help, usage := cli.HelpAndUsagePrinters(cli.CommandDocsForCommandString(commandStr, importDocs, ap))
	apr := cli.ParseArgsOrDie(ap, args, help)
	if !cli.CheckEnvIsValid(dEnv) {
		return 1
	}

	var verr errhand.VerboseError
	verr = validateImportArgs(apr)
	if verr != nil {
		return commands.HandleVErrAndExitCode(verr, usage)
	}

	path := apr.Arg(0)
	absPath, err := filepath.Abs(path)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}

	workflow, err := parseWorkflow(absPath)
	if err != nil {
		return commands.HandleVErrAndExitCode(errhand.VerboseErrorFromError(err), usage)
	}
	
	fmt.Fprintf(color.Output, "successfully parsed workflow: %s\n", workflow.Name)

	return 0
}

func parseWorkflow(path string) (workflow *dolt_ci.Workflow, err error) {
	var f *os.File
	f, err = os.Open(path)
	if err != nil {
		return
	}
	defer func() {
		rerr := f.Close()
		if err == nil {
			err = rerr
		}
	}()
	workflow, err = dolt_ci.ParseWorkflow(f)
	return
}

func validateImportArgs(apr *argparser.ArgParseResults) errhand.VerboseError {
	if apr.NArg() != 1 {
		return errhand.BuildDError("expected 1 argument").SetPrintUsage().Build()
	}
	return nil
}
