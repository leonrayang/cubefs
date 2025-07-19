package stream

import (
	"testing"

	"github.com/cubefs/cubefs/proto"
	"github.com/stretchr/testify/assert"
)

func TestWarmUpMetaPaths(t *testing.T) {
	rc := &RemoteCache{}
	t.Run("TestIsPathAlreadyCovered", func(t *testing.T) {
		parentPath := &proto.WarmUpPathInfo{
			DirPath: "/test",
			Status:  proto.WarmStatusInitializing,
		}
		rc.WarmUpMetaPaths.Store("/test", parentPath)
		assert.True(t, rc.isPathAlreadyCovered("/test/subdir"))
		assert.False(t, rc.isPathAlreadyCovered("/other"))
		rc.WarmUpMetaPaths.Delete("/test")
	})
}

func TestHasPathIntersection(t *testing.T) {
	tests := []struct {
		name     string
		dir1     string
		dir2     string
		expected bool
		result   string
	}{
		{
			name:     "Parent contains child",
			dir1:     "/parent",
			dir2:     "/parent/child",
			expected: true,
			result:   "/parent/child",
		},
		{
			name:     "Child contains parent",
			dir1:     "/parent/child",
			dir2:     "/parent",
			expected: true,
			result:   "/parent/child",
		},
		{
			name:     "No intersection",
			dir1:     "/path1",
			dir2:     "/path2",
			expected: false,
			result:   "",
		},
		{
			name:     "Same path",
			dir1:     "/same",
			dir2:     "/same",
			expected: true,
			result:   "/same",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hasIntersection, result := hasPathIntersection(tt.dir1, tt.dir2)
			assert.Equal(t, tt.expected, hasIntersection)
			if tt.expected {
				assert.Equal(t, tt.result, result)
			}
		})
	}
}
