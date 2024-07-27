package rowexec

import (
	"context"
	"encoding/binary"
	"github.com/dolthub/dolt/go/store/prolly"
	"github.com/dolthub/dolt/go/store/skip"
	"github.com/dolthub/dolt/go/store/val"
	"io"
)

func newSortedLookupSource(srcIter prolly.MapIter, keyTupleMapper *lookupMapping) prolly.MapIter {
	return &sortedLookupSource{
		srcIter:        srcIter,
		keyTupleMapper: keyTupleMapper,
	}
}

type sortedLookupSource struct {
	srcIter        prolly.MapIter
	keyTupleMapper *lookupMapping
	lookupKeyIter  *skip.ListIter
}

func (l *sortedLookupSource) loadSkipList(ctx context.Context) error {
	sl := skip.NewSkipList(func(i, j []byte) (cmp int) {
		return l.keyTupleMapper.targetKb.Desc.Compare(i, j)
	})
	for i := 0; i < 500; i++ {
		srcKey, srcVal, err := l.srcIter.Next(ctx)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if srcKey == nil {
			break
		}

		dstKey, err := l.keyTupleMapper.dstKeyTuple(ctx, srcKey, srcVal)
		if err != nil {
			return err
		}

		buf := make([]byte, 4+len(srcKey)+len(srcVal))
		binary.BigEndian.PutUint32(buf, uint32(len(srcKey)))
		copy(buf[4:], srcKey[:])
		copy(buf[4+len(srcKey):], srcVal[:])
		sl.Put(dstKey, buf)
	}
	l.lookupKeyIter = sl.IterAtStart()
	return nil
}

func (l *sortedLookupSource) Next(ctx context.Context) (val.Tuple, val.Tuple, error) {
	if l.lookupKeyIter == nil {
		if err := l.loadSkipList(ctx); err != nil {
			return nil, nil, err
		}
	}

	k, v := l.lookupKeyIter.Current()
	l.lookupKeyIter.Advance()
	return k, v, nil
}
