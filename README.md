# RAITO - DynamoDB utils

<p align="center">
    <a href="/LICENSE.md" target="_blank"><img src="https://img.shields.io/badge/license-Apache%202-brightgreen.svg?label=License" alt="Software License" /></a>
    <img src="https://img.shields.io/github/v/tag/raito-io/go-dynamo-utils?sort=semver&label=Version&color=651FFF" />
    <a href="https://github.com/raito-io/go-dynamo-utils/actions/workflows/build.yml" target="_blank"><img src="https://img.shields.io/github/actions/workflow/status/raito-io/go-dynamo-utils/build.yml?branch=main" alt="Build status" /></a>
    <a href="https://codecov.io/gh/raito-io/go-dynamo-utils" target="_blank"><img src="https://img.shields.io/codecov/c/github/raito-io/go-dynamo-utils?label=Coverage" alt="Code Coverage" /></a>
    <a href="https://github.com/raito-io/go-dynamo-utils/blob/master/CONTRIBUTING.md"><img src="https://img.shields.io/badge/Contribute-🙌-green.svg" /></a>
    <a href="https://golang.org/"><img src="https://img.shields.io/github/go-mod/go-version/raito-io/go-dynamo-utils?color=7fd5ea" /></a>
    <a href="https://pkg.go.dev/github.com/raito-io/go-dynamo-utils"><img src="https://pkg.go.dev/badge/github.com/raito-io/go-dynamo-utils.svg" alt="Go Reference"></a>
</p>

## Introduction
`dynamodb_utils` is a go library with utility functions for using aws dynamodb. 

## Getting Started
Add this library as a dependency via `go get github.com/raito-io/go-dynamo-utils`

## Features
- [Distributed lock](distrlock/README.md): Handle distributed locks by using DynamoDB
- [Executor](executor/README.md): Easy execution of Query and Scan operations on DynamoDB
- [Input Builder](inputbuilder/README.md): Build DynamoDB Scan, Query and Update input queries