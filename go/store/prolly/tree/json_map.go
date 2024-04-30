package tree

import (
	"bytes"
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/store/hash"
	"github.com/dolthub/dolt/go/store/prolly/message"
	"github.com/dolthub/dolt/go/store/val"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/dolthub/go-mysql-server/sql/types"
	"io"
)

type JPath []byte
type jValue []byte
type Lexicographic struct{}

type JSONStaticMap = StaticMap[JPath, jValue, Lexicographic]
type JSONMap JSONStaticMap
type JSONMapThreeWayDiffer = ThreeWayDiffer[JPath, Lexicographic]

func NewJSONThreeWayDiffer(ctx context.Context, ns NodeStore,
	leftMap JSONMap, rightMap JSONMap, baseMap JSONMap) (*JSONMapThreeWayDiffer, error) {
	resolveCb := func(*sql.Context, val.Tuple, val.Tuple, val.Tuple) (val.Tuple, bool, error) {
		return nil, true, fmt.Errorf("merge error")
	}
	return NewThreeWayDiffer[JPath, jValue, Lexicographic](ctx, ns,
		JSONStaticMap(leftMap), JSONStaticMap(rightMap), JSONStaticMap(baseMap), resolveCb,
		/*keyless*/ false,
		ThreeWayDiffInfo{},
		Lexicographic{})
}

func (l Lexicographic) Compare(left, right JPath) int {
	return bytes.Compare(left, right)
}

/*
type JSONMap struct {
	jsonMap
}*/

var _ sql.JSONWrapper = JSONMap{}

func NewJSONMapFromHash(ctx context.Context, addr hash.Hash, ns NodeStore) (*JSONMap, error) {
	root, err := ns.Read(ctx, addr)
	if err != nil {
		return nil, err
	}
	return &JSONMap{
		Root:      root,
		NodeStore: ns,
		Order:     Lexicographic{},
	}, nil
}

type prollyMapChunker = chunker[message.ProllyMapSerializer]

func serializeJsonValue(ctx context.Context, ns NodeStore, jsonChunker *prollyMapChunker, prefix string, v types.JsonObject) error {
	iter := types.NewJSONIter(v)
	for iter.HasNext() {
		key, value, err := iter.Next()
		if err != nil {
			return err
		}
		newKey := prefix + "." + key
		if object, ok := value.(types.JsonObject); ok {
			err = serializeJsonValue(ctx, ns, jsonChunker, newKey, object)
		}
		if array, ok := value.(types.JsonArray); ok {
			_ = array
		}
		if f, ok := value.(float64); ok {
			var floatBytes [9]byte
			floatBytes[0] = DOUBLE
			val.WriteFloat64(floatBytes[1:9], f)
			jsonChunker.AddPair(ctx, []byte(newKey), floatBytes[:])
		}

		/*switch value.(type) {
		default:
			panic("")
		}*/
		// Serialize(ctx, ns)
		_, _ = key, value
		// TODO: Do we need to wrap the bytes in a tuple?
		// chkr.AddPair(ctx, []byte(key))
	}
	return nil
}

func NewJSONMApFromGolangMap(ctx context.Context, ns NodeStore, v types.JsonObject) (hash.Hash, error) {
	var valDesc = val.NewTupleDescriptor(
		val.Type{Enc: val.ByteStringEnc, Nullable: false},
	)
	serializer := message.NewProllyMapSerializer(valDesc, ns.Pool())
	jsonChunker, err := newEmptyChunker(ctx, ns, serializer)
	if err != nil {
		return hash.Hash{}, err
	}

	err = serializeJsonValue(ctx, ns, jsonChunker, "$", v)
	if err != nil {
		return hash.Hash{}, err
	}

	// err = chkr.AddPair(ctx, Item(item[0]), Item(item[1]))

	nd, err := jsonChunker.Done(ctx)

	/*
		bb := ns.Write()
		bb.Init(dataSize)
		_, addr, err := bb.Chunk(ctx, r)
		if err != nil {
			return hash.Hash{}, err
		}*/
	jsonMap := JSONMap{
		Root:      nd,
		NodeStore: ns,
		Order:     Lexicographic{},
	}
	return JSONStaticMap(jsonMap).HashOf(), nil
}

func (j JSONMap) ToInterface() (interface{}, error) {
	// TODO: Pass in context?
	var ctx context.Context
	result := types.JSONDocument{Val: make(map[string]interface{})}
	iter, err := JSONStaticMap(j).IterAll(ctx)
	if err != nil {
		return nil, err
	}
	for key, value, err := iter.Next(ctx); ; key, value, err = iter.Next(ctx) {
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		tag := value[0]
		remainder := value[1:]
		vv, err := ResolveJson(ctx, tag, remainder, j.NodeStore)
		if err != nil {
			return nil, err
		}

		result.NestedInsert(string(key), types.JSONDocument{Val: vv})
		/*subObject := result
		for path := range paths.Lookup() {

		}*/
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
