package iodb

import (
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
)

var one = big.NewInt(1)

type metadata struct {
	Counter    *big.Int                     `json:"counter"`
	ExpiryDate map[string]int64             `json:"expiryDate,omitempty"`
	Extra      map[string]map[string]string `json:"extra,omitempty"`
	path       string
}

func (m *metadata) incCounter() *big.Int {
	return m.Counter.Add(m.Counter, one)
}

func (m *metadata) SetExpiryDate(path string, ts int64) {
	if ts == 0 && m.ExpiryDate != nil {
		delete(m.ExpiryDate, path)
		if len(m.ExpiryDate) == 0 {
			m.ExpiryDate = nil
		}
	} else {
		if m.ExpiryDate == nil {
			m.ExpiryDate = map[string]int64{}
		}
		m.ExpiryDate[path] = ts
	}

}

func (m *metadata) SetExtraData(path string, key string, val string) {
	if m.Extra == nil {
		if val == "" {
			return
		}
		m.Extra = make(map[string]map[string]string)
	}

	mm := m.Extra[path]
	if mm == nil {
		if val == "" {
			return
		}

		mm = map[string]string{}
		m.Extra[path] = mm
	}

	if val == "" {
		delete(mm, key)
		if len(mm) == 0 {
			delete(m.Extra, path)
		}
	} else {
		mm[key] = val
	}
}

func (m *metadata) CopyExtra(path string) (out map[string]string) {
	mm := m.Extra[path]
	out = make(map[string]string, len(mm))
	for k, v := range mm {
		out[k] = v
	}

	return
}

func (m *metadata) store() (err error) {
	var f *os.File
	tmpPath := m.path + ".tmp"
	if f, err = os.Create(tmpPath); err != nil {
		return err
	}
	if err = json.NewEncoder(f).Encode(m); err != nil {
		f.Close() // can't defer it because we wanna close it before the rename :|
		return err
	}
	if err = f.Close(); err != nil {
		return
	}
	return os.Rename(tmpPath, m.path)
}

func loadMetadata(p string) (*metadata, error) {
	p = filepath.Join(p, ".meta")
	m := &metadata{Counter: big.NewInt(0), path: p}
	f, err := os.Open(p)
	if err != nil {
		if os.IsNotExist(err) {
			return m, nil
		}
		return nil, err
	}

	if err = json.NewDecoder(f).Decode(m); err != nil {
		f.Close()
		return nil, err
	}

	if err = f.Close(); err != nil {
		return nil, err
	}

	return m, nil
}
