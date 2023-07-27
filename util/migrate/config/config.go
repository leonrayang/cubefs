package config

import (
	"bytes"
	"encoding/json"
	"github.com/pkg/errors"
	"io/ioutil"
)

func LoadConfig(conf interface{}, configPath string) error {
	data, err := ioutil.ReadFile(configPath)
	if err != nil {
		return errors.Wrapf(err, "%s read failed", configPath)
	}
	if err := loadConfigBytes(conf, data); err != nil {
		return errors.Wrapf(err, "%s load failed", configPath)
	}
	return nil
}

func loadConfigBytes(conf interface{}, data []byte) error {
	data = trimComments(data)
	if err := json.Unmarshal(data, conf); err != nil {
		return errors.Wrap(err, "parse failed")
	}
	return nil
}

func trimComments(data []byte) []byte {
	lines := bytes.Split(data, []byte("\n"))
	for k, line := range lines {
		lines[k] = trimCommentsLine(line)
	}
	return bytes.Join(lines, []byte("\n"))
}

func trimCommentsLine(line []byte) []byte {
	var newLine []byte
	var i, quoteCount int
	lastIdx := len(line) - 1
	for i = 0; i <= lastIdx; i++ {
		if line[i] == '\\' {
			if i != lastIdx && (line[i+1] == '\\' || line[i+1] == '"') {
				newLine = append(newLine, line[i], line[i+1])
				i++
				continue
			}
		}
		if line[i] == '"' {
			quoteCount++
		}
		if line[i] == '#' {
			if quoteCount%2 == 0 {
				break
			}
		}
		if line[i] == '/' && i < lastIdx && line[i+1] == '/' {
			if quoteCount%2 == 0 {
				break
			}
		}
		newLine = append(newLine, line[i])
	}
	return newLine
}
