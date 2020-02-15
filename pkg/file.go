package pkg

import (
	"io"
	"os"
	"os/user"
	"path/filepath"

	"github.com/docker/go-units"
)

// Returns full path of a file, "~" is replaced with home directory
func ExpandFilename(filename string) string {
	if filename == "" {
		return ""
	}

	if len(filename) > 2 && filename[:2] == "~/" {
		if usr, err := user.Current(); err == nil {
			filename = filepath.Join(usr.HomeDir, filename[2:])
		}
	}

	result, err := filepath.Abs(filename)

	if err != nil {
		panic(err)
	}

	return result + "/"
}

// CopyFile copies from src to dst until either EOF is reached
// on src or an error occurs. It verifies src exists and removes
// the dst if it exists.
func CopyfileRemove(src, dst string) (int64, error) {
	cleanSrc := filepath.Clean(src)
	cleanDst := filepath.Clean(dst)
	if cleanSrc == cleanDst {
		return 0, nil
	}
	sf, err := os.Open(cleanSrc)
	if err != nil {
		return 0, err
	}
	defer sf.Close()
	if err := os.Remove(cleanDst); err != nil && !os.IsNotExist(err) {
		return 0, err
	}
	df, err := os.Create(cleanDst)
	if err != nil {
		return 0, err
	}
	defer df.Close()
	defer os.Remove(src)

	return io.Copy(df, sf)
}

func HumanSize(size float64) string {
	abbr := []string{"B", "kB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB"}
	return units.CustomSize("%.4g %s", size, 1024.0, abbr)
}
