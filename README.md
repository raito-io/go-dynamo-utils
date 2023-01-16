# RAITO - DynamoDB utils

[![Go Report Card](https://goreportcard.com/badge/github.com/raito-io/go-dynamo-utils)](https://goreportcard.com/report/github.com/raito-io/go-dynamo-utils)
![Version](https://img.shields.io/github/v/tag/raito-io/go-dynamo-utils?sort=semver&label=version&color=651FFF)
[![Build](https://img.shields.io/github/actions/workflow/status/raito-io/go-dynamo-utils/build.yml?branch=main)](https://github.com/raito-io/go-dynamo-utils/actions/workflows/build.yml)
[![Coverage](https://img.shields.io/codecov/c/github/raito-io/go-dynamo-utils?label=coverage)](https://app.codecov.io/gh/raito-io/go-dynamo-utils)
[![Contribute](https://img.shields.io/badge/Contribute-🙌-green.svg)](/CONTRIBUTING.md)
[![Go version](https://img.shields.io/github/go-mod/go-version/raito-io/go-dynamo-utils?color=7fd5ea)](https://golang.org/)
[![Software License](https://img.shields.io/badge/license-Apache%202-brightgreen.svg?label=license)](/LICENSE)
[![Go Reference](https://pkg.go.dev/badge/github.com/raito-io/go-dynamo-utils.svg)](https://pkg.go.dev/github.com/raito-io/go-dynamo-utils)

## Introduction
`dynamodb_utils` is a go library with utility functions for using aws dynamodb. 

## Getting Started
Add this library as a dependency via `go get github.com/raito-io/go-dynamo-utils`

## Features
- [Distributed lock](distrlock/README.md): Handle distributed locks by using DynamoDB
- [Executor](executor/README.md): Easy execution of Query and Scan operations on DynamoDB
- [Input Builder](inputbuilder/README.md): Build DynamoDB Scan, Query and Update input queries