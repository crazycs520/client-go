package tikv

import (
	"github.com/tikv/client-go/v2/internal/logutil"
	"go.uber.org/zap"
)

func SetAppLogger(log *zap.Logger) {
	logutil.AppLogger = log
}
