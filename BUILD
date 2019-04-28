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
        "-Werror",
        "-Wsign-compare",
        "-Wno-unused-parameter",
        "-Wno-unused-variable",
        "-Woverloaded-virtual",
        "-Wnon-virtual-dtor",
        "-Wno-missing-field-initializers",
        "-std=c++11",
    ],
    linkopts = [
        "-lm",
        "-lpthread",
    ],
    deps = [
#        "//external:gflags",
#        "//external:glog",
#        "//external:gtest",
        "//external:brpc",
        "//external:protobuf",
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

