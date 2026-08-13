//go:build linux

package processlocal

import (
	"errors"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/lxk36/xgc2-orchestration-core/kernel/effect"
	"github.com/lxk36/xgc2-orchestration-core/kernel/execution"
	processport "github.com/lxk36/xgc2-orchestration-core/provider/process"
	"github.com/lxk36/xgc2-orchestration-core/sdk/go/contracts"
)

var environmentName = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

func validateDispatch(dispatch processport.Dispatch, action string) error {
	if err := effect.ValidateEnvelope(dispatch.Envelope); err != nil {
		return err
	}
	keyHash, err := execution.PrivateTokenDigest(dispatch.Envelope.IdempotencyKey)
	if err != nil || keyHash != dispatch.Envelope.IdempotencyKeyHash {
		return errors.New("process command idempotency key does not match its hash")
	}
	if len(dispatch.Envelope.RequiredCapabilityRefs) == 0 {
		if dispatch.Envelope.CapabilityToken != "" || dispatch.Envelope.CapabilityTokenHash != "" {
			return errors.New("process command without capabilities carries a token")
		}
	} else {
		tokenHash, err := execution.PrivateTokenDigest(dispatch.Envelope.CapabilityToken)
		if err != nil || tokenHash != dispatch.Envelope.CapabilityTokenHash {
			return errors.New("process command capability token does not match its hash")
		}
	}
	if dispatch.Envelope.Action != action || dispatch.Envelope.TargetRef == "" || dispatch.Envelope.Fence.Kind != contracts.FenceGeneration || dispatch.Envelope.Fence.Generation == nil {
		return errors.New("process command action, target, or generation fence is invalid")
	}
	fence := dispatch.Envelope.Fence.Generation
	if fence.BindingID != dispatch.Envelope.TargetRef {
		return errors.New("process command target and generation binding disagree")
	}
	if !contracts.ValidDigest(dispatch.AuthorizationDigest) || dispatch.At.IsZero() || !dispatch.At.Before(dispatch.Envelope.Deadline) {
		return errors.New("process command authorization or dispatch time is invalid")
	}
	if err := validateSpec(dispatch.Spec); err != nil {
		return err
	}
	if dispatch.Spec.DescriptorDigest != dispatch.Envelope.DescriptorDigest {
		return errors.New("process spec and command descriptor digests disagree")
	}
	if action == processport.ActionStart {
		if dispatch.Executable == "" || strings.TrimSpace(dispatch.Executable) != dispatch.Executable || strings.ContainsRune(dispatch.Executable, '\x00') {
			return errors.New("resolved executable is invalid")
		}
		if len(dispatch.Arguments) > 4096 || len(dispatch.Environment) > 4096 {
			return errors.New("resolved process arguments or environment exceed bounds")
		}
		for _, value := range append(append([]string(nil), dispatch.Arguments...), dispatch.Environment...) {
			if strings.ContainsRune(value, '\x00') {
				return errors.New("resolved process value contains NUL")
			}
		}
		for _, value := range dispatch.Environment {
			name, _, found := strings.Cut(value, "=")
			if !found || !environmentName.MatchString(name) {
				return errors.New("resolved environment entry is invalid")
			}
		}
		for _, path := range []string{dispatch.StdoutPath, dispatch.StderrPath} {
			if !filepath.IsAbs(path) || filepath.Clean(path) != path {
				return errors.New("process log path must be absolute and clean")
			}
		}
		if dispatch.WorkingDirectory != "" && (!filepath.IsAbs(dispatch.WorkingDirectory) || filepath.Clean(dispatch.WorkingDirectory) != dispatch.WorkingDirectory) {
			return errors.New("process working directory must be absolute and clean")
		}
	}
	if action == processport.ActionStop && dispatch.KnownIdentity == nil {
		return errors.New("stop command requires exact process identity")
	}
	return nil
}

func validateSpec(spec contracts.ProcessSpec) error {
	if !contracts.ValidVersion(spec.Version) {
		return errors.New("process spec version is invalid")
	}
	for _, value := range []string{
		spec.ProcessID, spec.ExecutableRef, spec.ParameterSetRef,
		spec.StdoutArtifactRef, spec.StderrArtifactRef,
	} {
		if !contracts.ValidIdentifier(value) {
			return errors.New("process spec identity is invalid")
		}
	}
	if !contracts.ValidDigest(spec.DescriptorDigest) || !contracts.ValidDigest(spec.DefinitionDigest) ||
		!contracts.ValidDigest(spec.ArgumentTemplateDigest) || !contracts.ValidDigest(spec.ParameterSetDigest) {
		return errors.New("process spec digest is invalid")
	}
	if spec.WorkingDirectoryRef != "" && !contracts.ValidIdentifier(spec.WorkingDirectoryRef) {
		return errors.New("process working directory ref is invalid")
	}
	for name, ref := range spec.EnvironmentRefs {
		if !environmentName.MatchString(name) || !contracts.ValidIdentifier(ref) {
			return errors.New("process environment ref is invalid")
		}
	}
	if spec.GracePeriodMillis < 10 || spec.GracePeriodMillis > uint64((10*time.Minute)/time.Millisecond) ||
		spec.KillWaitMillis < 10 || spec.KillWaitMillis > uint64((10*time.Minute)/time.Millisecond) {
		return errors.New("process grace or kill wait is outside bounds")
	}
	return nil
}

func openLog(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o600)
}
