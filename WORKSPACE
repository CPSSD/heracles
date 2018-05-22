http_archive(
    name = "io_bazel_rules_go",
    url = "https://github.com/bazelbuild/rules_go/releases/download/0.12.0/rules_go-0.12.0.tar.gz",
    sha256 = "c1f52b8789218bb1542ed362c4f7de7052abcf254d865d96fb7ba6d44bc15ee3",
)
load("@io_bazel_rules_go//go:def.bzl", "go_rules_dependencies", "go_register_toolchains",)
go_rules_dependencies()
go_register_toolchains()
load("@bazel_gazelle//:deps.bzl", "gazelle_dependencies")
gazelle_dependencies()

http_archive(
    name = "io_bazel_rules_docker",
    sha256 = "6dede2c65ce86289969b907f343a1382d33c14fbce5e30dd17bb59bb55bb6593",
    strip_prefix = "rules_docker-0.4.0",
    urls = ["https://github.com/bazelbuild/rules_docker/archive/v0.4.0.tar.gz"],
)
load(
    "@io_bazel_rules_docker//container:container.bzl",
    "container_pull",
    container_repositories = "repositories",
)
container_repositories()

# External Dependencies

load("@bazel_gazelle//:deps.bzl", "go_repository")

go_repository(
    name = "com_github_golang_glog",
    importpath = "github.com/golang/glog",
    commit = "23def4e6c14b4da8ac2ed8007337bc5eb5007998",
)

go_repository(
    name = "com_github_golang_protobuf",
    importpath = "github.com/golang/protobuf",
    tag = "v1.1.0",
)

go_repository(
    name = "com_github_pkg_errors",
    importpath = "github.com/pkg/errors",
    tag = "v0.8.0",
)

go_repository(
    name = "com_github_spf13_pflag",
    importpath = "github.com/spf13/pflag",
    tag = "v1.0.1",
)

go_repository(
    name = "com_github_spf13_viper",
    importpath = "github.com/spf13/viper",
    tag = "v1.0.2",
)

go_repository(
    name = "com_github_streadway_amqp",
    importpath = "github.com/streadway/amqp",
    commit = "8e4aba63da9fc5571e01c6a45dc809a58cbc5a68",
)

go_repository(
    name = "org_golang_x_net",
    importpath = "golang.org/x/net",
    remote = "git@github.com:golang/net",
    vcs = "git",
    commit = "f73e4c9ed3b7ebdd5f699a16a880c2b1994e50dd",
)

go_repository(
    name = "org_golang_google_grpc",
    importpath = "google.golang.org/grpc",
    tag = "v1.12.0",
)

go_repository(
    name = "com_github_google_uuid",
    importpath = "github.com/google/uuid",
    tag = "0.2",
)

go_repository(
    name = "com_github_urfave_cli",
    importpath = "github.com/urfave/cli",
    commit = "8e01ec4cd3e2d84ab2fe90d8210528ffbb06d8ff",
)

go_repository(
    name = "com_github_olekukonko_tablewriter",
    importpath = "github.com/olekukonko/tablewriter",
    commit = "d4647c9c7a84d847478d890b816b7d8b62b0b279",
)

# Dependencies of dependencies
go_repository(
    name = "com_github_fsnotify_fsnotify",
    importpath = "github.com/fsnotify/fsnotify",
    tag = "v1.4.7",
)

go_repository(
    name = "com_github_hashicorp_hcl",
    importpath = "github.com/hashicorp/hcl",
    commit = "ef8a98b0bbce4a65b5aa4c368430a80ddc533168",
)

go_repository(
    name = "com_github_spf13_jwalterweatherman",
    importpath = "github.com/spf13/jwalterweatherman",
    commit = "7c0cea34c8ece3fbeb2b27ab9b59511d360fb394",
)

go_repository(
    name = "com_github_magiconair_properties",
    importpath = "github.com/magiconair/properties",
    tag = "v1.8.0",
)

go_repository(
    name = "in_gopkg_yaml_v2",
    importpath = "gopkg.in/yaml.v2",
    tag = "v2.2.1",
)

go_repository(
    name = "com_github_mitchellh_mapstructure",
    importpath = "github.com/mitchellh/mapstructure",
    commit = "bb74f1db0675b241733089d5a1faa5dd8b0ef57b",
)

go_repository(
    name = "com_github_spf13_afero",
    importpath = "github.com/spf13/afero",
    tag = "v1.1.0",
)

go_repository(
    name = "com_github_spf13_cast",
    importpath = "github.com/spf13/cast",
    tag = "v1.2.0",
)

go_repository(
    name = "org_golang_x_sys",
    importpath = "golang.org/x/sys",
    remote = "git@github.com:golang/sys",
    vcs = "git",
    commit = "7c87d13f8e835d2fb3a70a2912c811ed0c1d241b",
)

go_repository(
    name = "com_github_mattn_go_runewidth",
    importpath = "github.com/mattn/go-runewidth",
    tag = "v0.0.2",
)