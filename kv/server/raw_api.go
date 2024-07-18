package server

import (
	"context"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// The functions below are Server's Raw API. (implements TinyKvServer).
// Some helper methods can be found in sever.go in the current directory

// RawGet return the corresponding Get response based on RawGetRequest's CF and Key fields
func (server *Server) RawGet(_ context.Context, req *kvrpcpb.RawGetRequest) (*kvrpcpb.RawGetResponse, error) {
	reader, err := server.storage.Reader(req.Context)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	val, err := reader.GetCF(req.GetCf(), req.GetKey())
	return &kvrpcpb.RawGetResponse{
		RegionError: nil,
		Error:       "",
		Value:       val,
		NotFound:    val == nil,
	}, nil
}

// RawPut puts the target data into storage and returns the corresponding response
func (server *Server) RawPut(_ context.Context, req *kvrpcpb.RawPutRequest) (*kvrpcpb.RawPutResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using Storage.Modify to store data to be modified
	err := server.storage.Write(nil, []storage.Modify{{
		Data: storage.Put{
			Key:   req.Key,
			Value: req.Value,
			Cf:    req.Cf,
		},
	}})

	return &kvrpcpb.RawPutResponse{
		RegionError: nil,
		Error:       "",
	}, err
}

// RawDelete delete the target data from storage and returns the corresponding response
func (server *Server) RawDelete(_ context.Context, req *kvrpcpb.RawDeleteRequest) (*kvrpcpb.RawDeleteResponse, error) {
	err := server.storage.Write(nil, []storage.Modify{{
		Data: storage.Delete{
			Key: req.Key,
			Cf:  req.Cf,
		},
	}})

	return &kvrpcpb.RawDeleteResponse{
		RegionError: nil,
		Error:       "",
	}, err
}

// RawScan scan the data starting from the start key up to limit. and return the corresponding result
func (server *Server) RawScan(_ context.Context, req *kvrpcpb.RawScanRequest) (*kvrpcpb.RawScanResponse, error) {
	// Your Code Here (1).
	// Hint: Consider using reader.IterCF

	reader, err := server.storage.Reader(nil)
	if err != nil {
		return &kvrpcpb.RawScanResponse{
			RegionError: nil,
			Error:       "",
			Kvs:         nil,
		}, err
	}
	iter := reader.IterCF(req.GetCf())
	nums := uint32(0)
	kvs := make([]*kvrpcpb.KvPair, 0)
	for iter.Seek(req.StartKey); iter.Valid(); iter.Next() {
		if nums >= req.GetLimit() {
			break
		}
		key := iter.Item().Key()
		val, _ := iter.Item().Value()
		kvs = append(kvs, &kvrpcpb.KvPair{
			Error: nil,
			Key:   key,
			Value: val,
		})
		nums++
	}
	return &kvrpcpb.RawScanResponse{
		RegionError: nil,
		Error:       "",
		Kvs:         kvs,
	}, nil
}
