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

package dolt_ci

import (
	"context"
	"fmt"
	"github.com/dolthub/dolt/go/libraries/doltcore/schema"
	"github.com/dolthub/dolt/go/libraries/doltcore/sqle/sqlutil"
	"github.com/dolthub/vitess/go/sqltypes"
	"strings"
	"unicode/utf8"
)

type ColumnValue struct {
	ColumnName string
	Value      string
}

const utf8RuneError = string(utf8.RuneError)

type ColumnValues []*ColumnValue

func toUtf8StringValue(ctx context.Context, col schema.Column, val interface{}) (string, error) {
	if val == nil {
		return "", nil
	} else if col.TypeInfo.ToSqlType().Type() == sqltypes.Blob {
		return "", fmt.Errorf("binary types not supported in dolt ci configuration")
	} else {
		formattedVal, err := sqlutil.SqlColToStr(col.TypeInfo.ToSqlType(), val)
		if err != nil {
			return "", err
		}

		if utf8.ValidString(formattedVal) {
			return formattedVal, nil
		} else {
			return strings.ToValidUTF8(formattedVal, utf8RuneError), nil
		}
	}
}

func NewColumnValue(ctx context.Context, col schema.Column, val interface{}) (*ColumnValue, error) {
	utf8Value, err := toUtf8StringValue(ctx, col, val)
	if err != nil {
		return nil, err
	}

	if utf8Value == "" {
		return nil, nil
	}

	return &ColumnValue{
		ColumnName: col.Name,
		Value:      utf8Value,
	}, nil
}
