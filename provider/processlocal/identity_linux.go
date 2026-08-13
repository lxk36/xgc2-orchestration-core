//go:build linux

package processlocal

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"

	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

type procIdentity struct {
	contracts.ProcessIdentity
	State byte
}

func readIdentity(pid int) (procIdentity, error) {
	if pid <= 0 {
		return procIdentity{}, errors.New("process pid must be positive")
	}
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return procIdentity{}, err
	}
	text := string(raw)
	closing := strings.LastIndexByte(text, ')')
	if closing < 0 || closing+2 >= len(text) {
		return procIdentity{}, errors.New("malformed process stat")
	}
	fields := strings.Fields(text[closing+2:])
	if len(fields) < 20 || len(fields[0]) != 1 {
		return procIdentity{}, errors.New("short process stat")
	}
	pgid, err := strconv.Atoi(fields[2])
	if err != nil {
		return procIdentity{}, err
	}
	startTicks, err := strconv.ParseUint(fields[19], 10, 64)
	if err != nil {
		return procIdentity{}, err
	}
	return procIdentity{ProcessIdentity: contracts.ProcessIdentity{PID: pid, PGID: pgid, StartTicks: startTicks}, State: fields[0][0]}, nil
}

func alive(identity contracts.ProcessIdentity) (bool, error) {
	if identity.PID <= 0 || identity.PGID <= 0 || identity.StartTicks == 0 {
		return false, errors.New("process identity is incomplete")
	}
	observed, err := readIdentity(identity.PID)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, syscall.ESRCH) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return observed.PID == identity.PID && observed.PGID == identity.PGID && observed.StartTicks == identity.StartTicks && observed.State != 'Z', nil
}

func signalGroup(identity contracts.ProcessIdentity, signal syscall.Signal) error {
	live, err := alive(identity)
	if err != nil || !live {
		return err
	}
	if err := syscall.Kill(-identity.PGID, signal); errors.Is(err, syscall.ESRCH) {
		return nil
	} else {
		return err
	}
}
