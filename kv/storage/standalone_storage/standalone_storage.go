package standalone_storage

import (
	"errors"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"

	"github.com/Connor1996/badger"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	opts := badger.DefaultOptions
	opts.Dir = conf.DBPath
	opts.ValueDir = conf.DBPath
	db, err := badger.Open(opts)

	if err != nil {
		log.Fatal(err)
	}
	return &StandAloneStorage{
		engine: engine_util.NewEngines(db, db, conf.DBPath, conf.DBPath),
	}
}

func (s *StandAloneStorage) Start() error {
	return nil
}

func (s *StandAloneStorage) Stop() error {
	return s.engine.Kv.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	return newStandAloneStorageReade(s.engine.Kv), nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	txn := s.engine.Kv.NewTransaction(true)
	for _, modify := range batch {
		var err error
		switch modify.Data.(type) {
		case storage.Put:
			err = txn.Set(engine_util.KeyWithCF(modify.Cf(), modify.Key()), modify.Value())
		case storage.Delete:
			err = txn.Delete(engine_util.KeyWithCF(modify.Cf(), modify.Key()))
			if errors.Is(err, badger.ErrKeyNotFound) {
				err = nil
			}
		}
		if err != nil {
			return err
		}
	}
	err := txn.Commit()
	return err
}

func newStandAloneStorageReade(db *badger.DB) storage.StorageReader {
	return standAloneStorageReader{
		txn: db.NewTransaction(false),
	}
}

type standAloneStorageReader struct {
	txn *badger.Txn
}

func (s standAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(s.txn, cf, key)
	if errors.Is(err, badger.ErrKeyNotFound) {
		err = nil
	}
	return val, err
}

func (s standAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, s.txn)
}

func (s standAloneStorageReader) Close() {
	if err := s.txn.Commit(); err != nil {
		log.Error(err)
	}
}
