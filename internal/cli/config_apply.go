package cli

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

// EnvPrefix is the prefix of the environment variables that supply flag
// values. A flag --page-size is read from PRISM_PAGE_SIZE.
const EnvPrefix = "PRISM_"

// ProjectConfigFile is the per-directory config file name, read from the
// current working directory.
const ProjectConfigFile = ".prism.yaml"

// configSources is everything applyConfigSources reads from the outside
// world, so tests can run it without touching the real environment or the
// real home directory.
type configSources struct {
	// explicit is the --config path. When set, it is the only file read and
	// it must exist.
	explicit string
	// userFile is the per-user config file, normally
	// <user config dir>/prism/config.yaml. Missing is fine.
	userFile string
	// projectFile is the per-directory config file, normally ./.prism.yaml.
	// Missing is fine.
	projectFile string
	// lookupEnv is os.LookupEnv outside tests.
	lookupEnv func(string) (string, bool)
}

// defaultConfigSources returns the real-world sources for an invocation.
func defaultConfigSources(explicit string) configSources {
	return configSources{
		explicit:    explicit,
		userFile:    DefaultConfigFile(),
		projectFile: ProjectConfigFile,
		lookupEnv:   os.LookupEnv,
	}
}

// EnvVarForFlag returns the environment variable that supplies a flag's
// value: PRISM_ followed by the flag name upper-cased, with dashes turned
// into underscores.
func EnvVarForFlag(name string) string {
	return EnvPrefix + strings.ToUpper(strings.ReplaceAll(name, "-", "_"))
}

// loadConfigFiles reads the config files for one invocation.
//
// With an explicit path only that file is read, and it must exist. Otherwise
// the per-user file is read first and the per-directory .prism.yaml is
// merged over it, so a project can override a user's defaults key by key.
// Neither file is required. A file that exists but cannot be parsed is an
// error: a misconfigured machine should fail loudly, not run on defaults.
func loadConfigFiles(src configSources) (*viper.Viper, []string, error) {
	v := viper.New()
	v.SetConfigType("yaml")

	if src.explicit != "" {
		v.SetConfigFile(src.explicit)
		if err := v.ReadInConfig(); err != nil {
			return nil, nil, fmt.Errorf("reading config file %s: %w", src.explicit, err)
		}
		return v, []string{src.explicit}, nil
	}

	var used []string
	for _, path := range []string{src.userFile, src.projectFile} {
		if path == "" {
			continue
		}
		info, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return nil, nil, fmt.Errorf("reading config file %s: %w", path, err)
		}
		if info.IsDir() {
			return nil, nil, fmt.Errorf("reading config file %s: is a directory", path)
		}
		v.SetConfigFile(path)
		if err := v.MergeInConfig(); err != nil {
			return nil, nil, fmt.Errorf("reading config file %s: %w", filepath.Clean(path), err)
		}
		used = append(used, path)
	}
	return v, used, nil
}

// applyConfigSources fills in every flag of cmd that was not given on the
// command line, so the documented precedence holds for all of them:
//
//	CLI flag  >  PRISM_<FLAG> environment variable  >  config file  >  default
//
// Within the config files, a key under the command's own section (for
// example events.page-size) wins over the same key at the top level, and
// ./.prism.yaml wins over the per-user file.
//
// Values are applied through the flag set, so they are parsed and validated
// exactly as if they had been typed, and are marked as changed: a value from
// the environment or a file behaves like the flag in every check that asks
// whether a flag was set.
func applyConfigSources(cmd *cobra.Command, src configSources) error {
	v, _, err := loadConfigFiles(src)
	if err != nil {
		return err
	}
	lookupEnv := src.lookupEnv
	if lookupEnv == nil {
		lookupEnv = os.LookupEnv
	}

	section := commandSection(cmd)
	flags := cmd.Flags()

	var errs []error
	flags.VisitAll(func(f *pflag.Flag) {
		if f.Changed || f.Name == "help" || f.Name == "config" {
			return
		}

		envName := EnvVarForFlag(f.Name)
		if raw, ok := lookupEnv(envName); ok && strings.TrimSpace(raw) != "" {
			if err := setFlagValues(flags, f, splitEnvValue(f, raw)); err != nil {
				errs = append(errs, fmt.Errorf("$%s: %w", envName, err))
			}
			return
		}

		key, ok := configKey(v, section, f.Name)
		if !ok {
			return
		}
		values, err := configValues(v.Get(key))
		if err != nil {
			errs = append(errs, fmt.Errorf("config key %q: %w", key, err))
			return
		}
		if err := setFlagValues(flags, f, values); err != nil {
			errs = append(errs, fmt.Errorf("config key %q: %w", key, err))
		}
	})
	return errors.Join(errs...)
}

// commandSection is the config section that holds a command's own keys: the
// command path below the root, joined with dots ("events", or
// "completion.bash" for a nested command).
func commandSection(cmd *cobra.Command) string {
	var parts []string
	for c := cmd; c != nil && c.HasParent(); c = c.Parent() {
		parts = append([]string{c.Name()}, parts...)
	}
	return strings.Join(parts, ".")
}

// configKey finds the most specific config key for a flag: the command's
// section first, then the top level.
func configKey(v *viper.Viper, section, flag string) (string, bool) {
	if section != "" {
		if key := section + "." + flag; v.IsSet(key) {
			return key, true
		}
	}
	if v.IsSet(flag) {
		// A top-level key that is itself a map is a command section, not a
		// value (for example a flag and a command that share a name).
		if _, isMap := v.Get(flag).(map[string]any); isMap {
			return "", false
		}
		return flag, true
	}
	return "", false
}

// configValues turns a YAML value into the string form a flag parses. A list
// becomes one value per element, which is how repeatable flags are set.
func configValues(raw any) ([]string, error) {
	switch val := raw.(type) {
	case nil:
		return nil, nil
	case []any:
		out := make([]string, 0, len(val))
		for _, item := range val {
			if _, nested := item.(map[string]any); nested {
				return nil, fmt.Errorf("list items must be scalars")
			}
			out = append(out, fmt.Sprint(item))
		}
		return out, nil
	case []string:
		return val, nil
	case map[string]any:
		return nil, fmt.Errorf("expected a scalar or a list, got a mapping")
	default:
		return []string{fmt.Sprint(val)}, nil
	}
}

// splitEnvValue splits an environment value for a repeatable flag. Values of
// repeatable flags are separated by newlines, because several of them (topic
// filters, headers) legitimately contain commas. Flags whose command-line form
// already splits on commas keep doing so when Set is called.
func splitEnvValue(f *pflag.Flag, raw string) []string {
	if !isRepeatable(f) {
		return []string{strings.TrimSpace(raw)}
	}
	var out []string
	for _, line := range strings.Split(raw, "\n") {
		if line = strings.TrimSpace(strings.TrimSuffix(line, "\r")); line != "" {
			out = append(out, line)
		}
	}
	return out
}

func isRepeatable(f *pflag.Flag) bool {
	_, ok := f.Value.(pflag.SliceValue)
	return ok
}

// setFlagValues applies values through the flag set, so they are parsed as
// if typed and the flag is marked as changed. A scalar flag takes exactly one
// value.
func setFlagValues(flags *pflag.FlagSet, f *pflag.Flag, values []string) error {
	if len(values) == 0 {
		return nil
	}
	if !isRepeatable(f) && len(values) > 1 {
		return fmt.Errorf("--%s takes a single value, got %d", f.Name, len(values))
	}
	for _, value := range values {
		if err := flags.Set(f.Name, value); err != nil {
			return fmt.Errorf("invalid value %q for --%s: %w", value, f.Name, err)
		}
	}
	return nil
}
