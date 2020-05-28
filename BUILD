licenses(["notice"])

exports_files(["LICENSE"])

load(":bazel/braft.bzl", "braft_proto_library")

cc_library(
    name = "braft",
    srcs = glob([
        "src/braft/*.cpp",
    ]),
    hdrs = glob([
        "src/braft/*.h",
    ]),
    includes = [
        "src",
    ],
    defines = [],
    copts = [
        "-DGFLAGS=gflags",
        "-DOS_LINUX",
        "-DSNAPPY",
        "-DHAVE_SSE42",
        "-DNDEBUG",
        "-D__STDC_FORMAT_MACROS",
        "-fno-omit-frame-pointer",
        "-momit-leaf-frame-pointer",
        "-msse4.2",
        "-pthread",
        "-Wsign-compare",
        "-Wno-unused-parameter",
        "-Wno-unused-variable",
        "-Woverloaded-virtual",
        "-Wnon-virtual-dtor",
        "-Wno-missing-field-initializers",
        "-std=c++11",
        "-DGFLAGS_NS=google",
    ],
    linkopts = [
        "-lm",
        "-lpthread",
    ],
    deps = [
        "@com_github_brpc_brpc//:brpc",
        "@com_github_gflags_gflags//:gflags",
        "@com_github_google_glog//:glog",
        "@com_google_protobuf//:protobuf",        
        ":cc_braft_internal_proto",
    ],
    visibility = ["//visibility:public"],
)

braft_proto_library(
    name = "cc_braft_internal_proto",
    srcs = glob([
        "src/braft/*.proto",
    ]),
    include = "src",
    visibility = ["//visibility:public"],
)
