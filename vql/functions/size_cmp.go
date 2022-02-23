package functions

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"unicode"

	"github.com/Velocidex/ordereddict"
	vql_subsystem "www.velocidex.com/golang/velociraptor/vql"
	"www.velocidex.com/golang/vfilter"
	"www.velocidex.com/golang/vfilter/arg_parser"
)

func getOperators() []string {
	return []string{"eq", "le", "ge", "gt", "lt"}
}

const (
	BYTE = 1 << (10 * iota)
	KILOBYTE
	MEGABYTE
	GIGABYTE
	TERABYTE
	PETABYTE
	EXABYTE
)

func sizeByte(strSize string) (int64, error) {
	strSize = strings.TrimSpace(strSize)
	strSize = strings.ToUpper(strSize)

	i := strings.IndexFunc(strSize, unicode.IsLetter)

	if i == -1 {
		return 0, nil
	}

	bytesString, multiple := strSize[:i], strSize[i:]
	bytes, err := strconv.ParseFloat(bytesString, 64)
	if err != nil || bytes < 0 {
		return 0, errors.New("size must be a postive integer")
	}

	switch multiple {
	case "E", "EB", "EIB":
		return int64(bytes * EXABYTE), nil
	case "P", "PB", "PIB":
		return int64(bytes * PETABYTE), nil
	case "T", "TB", "TIB":
		return int64(bytes * TERABYTE), nil
	case "G", "GB", "GIB":
		return int64(bytes * GIGABYTE), nil
	case "M", "MB", "MIB":
		return int64(bytes * MEGABYTE), nil
	case "K", "KB", "KIB":
		return int64(bytes * KILOBYTE), nil
	case "B":
		return int64(bytes), nil
	default:
		return 0, errors.New("size must be a positive integer")
	}
}

type SizeCmpArgs struct {
	Path     string `vfilter:"required,field=path,default=/var/log,doc=The location to check recursivly for files"`
	Size     string `vfilter:"field=size,doc=The size of which file is greater or equal than"`
	Operator string `vfilter:"field=operator,doc=Operator which should be used."`
}

type SizeCmpFunction struct{}

func (self SizeCmpFunction) Info(scope vfilter.Scope, type_map *vfilter.TypeMap) *vfilter.FunctionInfo {
	return &vfilter.FunctionInfo{
		Name:    "size_cmp",
		Doc:     "Queries for files greater than given size and given directory.",
		ArgType: type_map.AddType(scope, &SizeCmpArgs{}),
	}
}

type SizeCmp string

func (self SizeCmpFunction) Call(ctx context.Context, scope vfilter.Scope, args *ordereddict.Dict) vfilter.Any {
	myArgs := &SizeCmpArgs{}
	err := arg_parser.ExtractArgsWithContext(ctx, scope, args, myArgs)
	if err != nil {
		scope.Log("size_cmp(%s,%s,%s): %s", myArgs.Path, myArgs.Size, myArgs.Operator, err.Error())
		return nil
	}
	if myArgs.Operator == "" {
		myArgs.Operator = "eq"
	}
	if myArgs.Size == "" {
		myArgs.Size = "0"
	}
	// check for validity of operator
	opValid := false
	for _, op := range getOperators() {
		if op == myArgs.Operator {
			opValid = true
		}
	}
	if !opValid {
		scope.Log("size_cmp(%s,%s,%s): invalid operator %s",
			myArgs.Path, myArgs.Size, myArgs.Operator, myArgs.Operator)
		return nil
	}
	// check if path is valid
	stat, err := os.Stat(myArgs.Path)
	if err != nil {
		scope.Log("size_cmp(%s,%s,%s): %s", myArgs.Path, myArgs.Size, myArgs.Operator, err.Error())
		return nil
	}
	if !stat.IsDir() {
		scope.Log("size_cmp(%s,%s,%s): is not a directory", myArgs.Path, myArgs.Size, myArgs.Operator)
		return nil
	}
	// transform size
	size, err := sizeByte(myArgs.Size)
	if err != nil {
		scope.Log("size_cmp(%s,%s,%s): %s", myArgs.Path, myArgs.Size, err.Error())
		return nil
	}
	var fileList []string
	filepath.Walk(myArgs.Path, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			scope.Log("size_cmp(%s,%s,%s): %s", myArgs.Path, myArgs.Size, myArgs.Operator, err.Error())
			return nil
		}
		if !info.IsDir() {
			// if a not regular file (softlinke,FIFO etc.) is found, log it, also append to output
			// should the user care
			if !info.Mode().IsRegular() {
				scope.Log("size_cmp(%s,%s,%s): %s is %s", myArgs.Path, myArgs.Size, myArgs.Operator, info.Name(), info.Mode())
				fileList = append(fileList, info.Name())

			} else {
				switch myArgs.Operator {
				case "eq":
					if info.Size() == size {
						fileList = append(fileList, info.Name())
					}
				case "lt":
					if info.Size() < size {
						fileList = append(fileList, info.Name())
					}
				case "gt":
					if info.Size() > size {
						fileList = append(fileList, info.Name())
					}
				case "le":
					if info.Size() <= size {
						fileList = append(fileList, info.Name())
					}
				case "ge":
					if info.Size() >= size {
						fileList = append(fileList, info.Name())
					}
				}
			}

		}
		return nil
	})
	return "foo"
}

func init() {
	vql_subsystem.RegisterFunction(&SizeCmpFunction{})
}
