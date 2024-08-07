package worker

// Get value or default
func getValue(m interface{}, key string, def interface{}) interface{} {
	if sm, ok := m.(map[string]interface{}); ok {
		if value, ok := sm[key]; ok {
			return value
		}
	}

	return def
}

// Try cast to string or default
func tryString(v interface{}, def string) string {
	if s, ok := v.(string); ok {
		return s
	}

	return def
}

// Try get string value or default
func tryGetString(m interface{}, key string, def string) string {
	return tryString(getValue(m, key, def), def)
}

// Try cast to string or default
func tryUInt64(v interface{}, def uint64) uint64 {
	if s, ok := v.(uint64); ok {
		return s
	}

	return def
}

// Try get string value or default
func tryGetUInt64(m interface{}, key string, def uint64) uint64 {
	return tryUInt64(getValue(m, key, def), def)
}

// Try cast to map[string] or default
func tryStringMap(v interface{}, def map[string]interface{}) map[string]interface{} {
	if s, ok := v.(map[string]interface{}); ok {
		return s
	}

	return def
}

// Try get map[string] value or default
func tryGetStringMap(m interface{}, key string, def map[string]interface{}) map[string]interface{} {
	return tryStringMap(getValue(m, key, def), def)
}
