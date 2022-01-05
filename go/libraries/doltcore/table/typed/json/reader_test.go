// Copyright 2019 Dolthub, Inc.
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

package json

import (
	"context"
	"io"
	"testing"

	"github.com/dolthub/go-mysql-server/enginetest"
	"github.com/dolthub/go-mysql-server/sql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dolthub/dolt/go/libraries/doltcore/row"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema/typeinfo"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/dolt/go/libraries/utils/filesys"
	"github.com/dolthub/dolt/go/store/types"
)

func TestReader(t *testing.T) {
	testJSON := `{
		"rows": [
			 {
			   "id": 0,
			   "first name": "tim",
			   "last name": "sehn"
			},
			{
			   "id": 1,
			   "first name": "brian",
			   "last name": "hendriks"
			}
		]
	}`

	fs := filesys.EmptyInMemFS("/")
	require.NoError(t, fs.WriteFile("file.json", []byte(testJSON)))

	colColl := schema.NewColCollection(
		schema.Column{
			Name:       "id",
			Tag:        0,
			Kind:       types.IntKind,
			IsPartOfPK: true,
			TypeInfo:   typeinfo.Int64Type,
		},
		schema.Column{
			Name:       "first name",
			Tag:        1,
			Kind:       types.StringKind,
			IsPartOfPK: false,
			TypeInfo:   typeinfo.StringDefaultType,
		},
		schema.Column{
			Name:       "last name",
			Tag:        2,
			Kind:       types.StringKind,
			IsPartOfPK: false,
			TypeInfo:   typeinfo.StringDefaultType,
		},
	)

	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)

	sqlSch, err := sqlutil.FromDoltSchema("", sch)
	require.NoError(t, err)

	vrw := types.NewMemoryValueStore()
	reader, err := OpenJSONReader(vrw, "file.json", fs, sch)
	require.NoError(t, err)

	verifySchema, err := reader.VerifySchema(sch)
	require.NoError(t, err)
	assert.True(t, verifySchema)

	var rows []sql.Row
	for {
		r, err := reader.ReadSqlRow(context.Background())
		if err == io.EOF {
			break
		} else {
			require.NoError(t, err)
		}
		rows = append(rows, r)
	}

	expectedRows := []sql.Row{
		{0, "tim", "sehn"},
		{1, "brian", "hendriks"},
	}

	assert.Equal(t, enginetest.WidenRows(sqlSch.Schema, expectedRows), rows)
}

func TestReaderBadJson(t *testing.T) {
	testJSON := ` {
   "rows": [
   {
   "id": 0,
   "first name": "tim",
   "last name": "sehn"
   bad
 },
 {
   "id": 1,
   "first name": "aaron",
   "last name": "son",
 },
 {
   "id": 2,
   "first name": "brian",
   "last name": "hendricks",
 }
 }
]
}`

	fs := filesys.EmptyInMemFS("/")
	require.NoError(t, fs.WriteFile("file.json", []byte(testJSON)))

	colColl := schema.NewColCollection(
		schema.Column{
			Name:       "id",
			Tag:        0,
			Kind:       types.IntKind,
			IsPartOfPK: true,
			TypeInfo:   typeinfo.Int64Type,
		},
		schema.Column{
			Name:       "first name",
			Tag:        1,
			Kind:       types.StringKind,
			IsPartOfPK: false,
			TypeInfo:   typeinfo.StringDefaultType,
		},
		schema.Column{
			Name:       "last name",
			Tag:        2,
			Kind:       types.StringKind,
			IsPartOfPK: false,
			TypeInfo:   typeinfo.StringDefaultType,
		},
	)

	sch, err := schema.SchemaFromCols(colColl)
	require.NoError(t, err)

	vrw := types.NewMemoryValueStore()
	reader, err := OpenJSONReader(vrw, "file.json", fs, sch)
	require.NoError(t, err)

	err = nil
	for {
		_, err = reader.ReadSqlRow(context.Background())
		if err != nil {
			break
		}
	}
	assert.NotEqual(t, io.EOF, err)
	assert.Error(t, err)
}

func newRow(sch schema.Schema, id int, first, last string) row.Row {
	vals := row.TaggedValues{
		0: types.Int(id),
		1: types.String(first),
		2: types.String(last),
	}

	r, err := row.New(types.Format_LD_1, sch, vals)

	if err != nil {
		panic(err)
	}

	return r
}
