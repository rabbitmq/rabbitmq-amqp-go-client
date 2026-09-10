#!/bin/bash

version=$1
gpg_key=$2
regex="^([0-9]+)\.([0-9]+)\.([0-9]+)(-(alpha|beta|rc)\.[0-9]+)?$"
tag="v$version"

if [ $# -lt 2 ]; then
    echo "Usage: $0 <version> <gpg_key>"
    exit 1
fi

if [[ ! $version =~ $regex ]]; then
    echo "Invalid version format: $version"
    exit 1
fi

echo "Updating ClientVersion to $version"
sed -i -e "s/ClientVersion = \".*\"/ClientVersion = \"$version\"/" pkg/rabbitmqamqp/common.go
go fmt ./...

echo ""
echo "Committing changes"
git add pkg/rabbitmqamqp/common.go
git commit -m "rabbitmq-amqp-go-client $tag"

read -p "Push the last commit to the main branch now? [y/N] " push_answer
if [[ "$push_answer" =~ ^[Yy]$ ]]; then
    echo "Pushing to main"
    git push
else
    echo "Skipping push of the commit."
fi

echo ""
echo "Creating and pushing tag $tag"
git tag -a -s -u $gpg_key -m "rabbitmq-amqp-go-client $tag" $tag && git push --tags
