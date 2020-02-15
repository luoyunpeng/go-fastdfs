package pkg

import "errors"

func CheckScene(scene string, scenes []string) (bool, error) {
	// no scene given, return true
	if len(scenes) == 0 {
		return true, nil
	}

	for _, s := range scenes {
		if scene == s {
			return true, nil
		}
	}

	return false, errors.New("not valid scene")
}
