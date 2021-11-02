// Copyright 2021 Dolthub, Inc.
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

package val

import "github.com/dolthub/dolt/go/store/pool"

type TupleBuilder struct {
	Desc TupleDesc

	buf [MaxTupleDataSize]byte
	off ByteSize

	fields [MaxTupleFields][]byte
}

func NewTupleBuilder(desc TupleDesc) *TupleBuilder {
	return &TupleBuilder{Desc: desc}
}

// Tuple materializes a Tuple from the fields written to the TupleBuilder.
func (tb *TupleBuilder) Tuple(pool pool.BuffPool) (tup Tuple) {
	for i, typ := range tb.Desc.Types {
		if !typ.Nullable && tb.fields[i] == nil {
			panic("cannot write NULL to non-NULL field")
		}
	}

	values := tb.fields[:tb.Desc.Count()]
	tup = NewTuple(pool, values...)
	tb.Recycle()
	return
}

// Recycle resets the TupleBuilder so it can build a new Tuple.
func (tb *TupleBuilder) Recycle() {
	tb.off = 0
}

// PutNull writes a NULL to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutNull(i int, v bool) {
}

// PutBool writes a bool to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutBool(i int, v bool) {
	tb.Desc.expectEncoding(i, Int8Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+1]
	writeBool(tb.fields[i], v)
	tb.off += 1
}

// PutInt8 writes an int8 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutInt8(i int, v int8) {
	tb.Desc.expectEncoding(i, Int8Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+1]
	writeInt8(tb.fields[i], v)
	tb.off += 1
}

// PutUint8 writes a uint8 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutUint8(i int, v uint8) {
	tb.Desc.expectEncoding(i, Uint8Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+1]
	writeUint8(tb.fields[i], v)
	tb.off += 1
}

// PutInt16 writes an int16 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutInt16(i int, v int16) {
	tb.Desc.expectEncoding(i, Int16Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+2]
	writeInt16(tb.fields[i], v)
	tb.off += 2
}

// PutUint16 writes a uint16 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutUint16(i int, v uint16) {
	tb.Desc.expectEncoding(i, Uint16Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+2]
	writeUint16(tb.fields[i], v)
	tb.off += 2
}

// PutInt32 writes an int32 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutInt32(i int, v int32) {
	tb.Desc.expectEncoding(i, Int32Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+4]
	writeInt32(tb.fields[i], v)
	tb.off += 4
}

// PutUint32 writes a uint32 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutUint32(i int, v uint32) {
	tb.Desc.expectEncoding(i, Uint32Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+4]
	writeUint32(tb.fields[i], v)
	tb.off += 4
}

// PutInt64 writes an int64 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutInt64(i int, v int64) {
	tb.Desc.expectEncoding(i, Int64Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+8]
	writeInt64(tb.fields[i], v)
	tb.off += 8
}

// PutUint64 writes a uint64 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutUint64(i int, v uint64) {
	tb.Desc.expectEncoding(i, Uint64Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+8]
	writeUint64(tb.fields[i], v)
	tb.off += 8
}

// PutFloat32 writes a float32 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutFloat32(i int, v float32) {
	tb.Desc.expectEncoding(i, Float32Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+4]
	writeFloat32(tb.fields[i], v)
	tb.off += 4
}

// PutFloat64 writes a float64 to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutFloat64(i int, v float64) {
	tb.Desc.expectEncoding(i, Float64Enc)
	tb.fields[i] = tb.buf[tb.off : tb.off+8]
	writeFloat64(tb.fields[i], v)
	tb.off += 8
}

// PutString writes a string to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutString(i int, v string) {
	tb.Desc.expectEncoding(i, StringEnc)
	sz := ByteSize(len(v))
	tb.fields[i] = tb.buf[tb.off : tb.off+sz]
	writeString(tb.fields[i], v, tb.Desc.Types[i].Coll)
	tb.off += sz
}

// PutBytes writes a []byte to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutBytes(i int, v []byte) {
	tb.Desc.expectEncoding(i, BytesEnc)
	sz := ByteSize(len(v))
	tb.fields[i] = tb.buf[tb.off : tb.off+sz]
	writeBytes(tb.fields[i], v, tb.Desc.Types[i].Coll)
	tb.off += sz
}

// PutField writes an interface{} to the ith field of the Tuple being built.
func (tb *TupleBuilder) PutField(i int, v interface{}) {
	switch tb.Desc.Types[i].Enc {
	case Int8Enc:
		tb.PutInt8(i, int8(convInt(v)))
	case Uint8Enc:
		tb.PutUint8(i, uint8(convUint(v)))
	case Int16Enc:
		tb.PutInt16(i, int16(convInt(v)))
	case Uint16Enc:
		tb.PutUint16(i, uint16(convUint(v)))
	case Int24Enc:
		panic("24 bit")
	case Uint24Enc:
		panic("24 bit")
	case Int32Enc:
		tb.PutInt32(i, int32(convInt(v)))
	case Uint32Enc:
		tb.PutUint32(i, uint32(convUint(v)))
	case Int64Enc:
		tb.PutInt64(i, int64(convInt(v)))
	case Uint64Enc:
		tb.PutUint64(i, uint64(convUint(v)))
	case Float32Enc:
		tb.PutFloat32(i, v.(float32))
	case Float64Enc:
		tb.PutFloat64(i, v.(float64))
	case StringEnc:
		tb.PutString(i, v.(string))
	case BytesEnc:
		tb.PutBytes(i, v.([]byte))
	default:
		panic("unknown encoding")
	}
}

func convInt(v interface{}) int {
	switch i := v.(type) {
	case int8:
		return int(i)
	case uint8:
		return int(i)
	case int16:
		return int(i)
	case uint16:
		return int(i)
	case int32:
		return int(i)
	case uint32:
		return int(i)
	case int64:
		return int(i)
	case uint64:
		return int(i)
	default:
		panic("impossible conversion")
	}
}

func convUint(v interface{}) uint {
	switch i := v.(type) {
	case int8:
		return uint(i)
	case uint8:
		return uint(i)
	case int16:
		return uint(i)
	case uint16:
		return uint(i)
	case int32:
		return uint(i)
	case uint32:
		return uint(i)
	case int64:
		return uint(i)
	case uint64:
		return uint(i)
	default:
		panic("impossible conversion")
	}
}
