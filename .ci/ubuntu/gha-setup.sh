#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o xtrace

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
readonly script_dir
echo "[INFO] script_dir: '$script_dir'"
readonly rabbitmq_image=rabbitmq:4.1.0-beta.4-management-alpine


readonly docker_name_prefix='rabbitmq-amqp-go-client'
readonly docker_network_name="$docker_name_prefix-network"

if [[ ! -v GITHUB_ACTIONS ]]
then
    GITHUB_ACTIONS='false'
fi

if [[ -d $GITHUB_WORKSPACE ]]
then
    echo "[INFO] GITHUB_WORKSPACE is set: '$GITHUB_WORKSPACE'"
else
    GITHUB_WORKSPACE="$(cd "$script_dir/../.." && pwd)"
    echo "[INFO] set GITHUB_WORKSPACE to: '$GITHUB_WORKSPACE'"
fi

if [[ $1 == 'toxiproxy' ]]
then
    readonly run_toxiproxy='true'
else
    readonly run_toxiproxy='false'
fi

if [[ $2 == 'pull' ]]
then
    readonly docker_pull_args='--pull always'
else
    readonly docker_pull_args=''
fi

if [[ $1 == 'stop' ]]
then
    docker stop "$rabbitmq_docker_name"
    docker stop "$toxiproxy_docker_name"
    exit 0
fi

set -o nounset

declare -r rabbitmq_docker_name="$docker_name_prefix-rabbitmq"
declare -r toxiproxy_docker_name="$docker_name_prefix-toxiproxy"

function start_toxiproxy
{
    if [[ $run_toxiproxy == 'true' ]]
    then
        # sudo ss -4nlp
        echo "[INFO] starting Toxiproxy server docker container"
        docker rm --force "$toxiproxy_docker_name" 2>/dev/null || echo "[INFO] $toxiproxy_docker_name was not running"
        # shellcheck disable=SC2086
        docker run --detach $docker_pull_args \
            --name "$toxiproxy_docker_name" \
            --hostname "$toxiproxy_docker_name" \
            --publish 8474:8474 \
            --publish 55670-55680:55670-55680 \
            --network "$docker_network_name" \
            'ghcr.io/shopify/toxiproxy:latest'
    fi
}

function start_rabbitmq
{
    echo "[INFO] starting RabbitMQ server docker container"
    chmod 0777 "$GITHUB_WORKSPACE/.ci/ubuntu/log"
    docker rm --force "$rabbitmq_docker_name" 2>/dev/null || echo "[INFO] $rabbitmq_docker_name was not running"
    # shellcheck disable=SC2086
    docker run --detach $docker_pull_args \
        --name "$rabbitmq_docker_name" \
        --hostname "$rabbitmq_docker_name" \
        --publish 5671:5671 \
        --publish 5672:5672 \
        --publish 15672:15672 \
        --network "$docker_network_name" \
        --volume "$GITHUB_WORKSPACE/.ci/ubuntu/enabled_plugins:/etc/rabbitmq/enabled_plugins" \
        --volume "$GITHUB_WORKSPACE/.ci/ubuntu/rabbitmq.conf:/etc/rabbitmq/rabbitmq.conf:ro" \
         --volume "$GITHUB_WORKSPACE/.ci/ubuntu/definitions.json:/etc/rabbitmq/definitions.json:ro" \
        --volume "$GITHUB_WORKSPACE/.ci/ubuntu/advanced.config:/etc/rabbitmq/advanced.config:ro" \
        --volume "$GITHUB_WORKSPACE/.ci/certs:/etc/rabbitmq/certs:ro" \
        --volume "$GITHUB_WORKSPACE/.ci/ubuntu/log:/var/log/rabbitmq" \
        "$rabbitmq_image"
}

function wait_rabbitmq
{
    set +o errexit
    set +o xtrace

    declare -i count=12
    while (( count > 0 )) && [[ "$(docker inspect --format='{{.State.Running}}' "$rabbitmq_docker_name")" != 'true' ]]
    do
        echo '[WARNING] RabbitMQ container is not yet running...'
        sleep 5
        (( count-- ))
    done

    declare -i count=12
    while (( count > 0 )) && ! docker exec "$rabbitmq_docker_name" epmd -names | grep -F 'name rabbit'
    do
        echo '[WARNING] epmd is not reporting rabbit name just yet...'
        sleep 5
        (( count-- ))
    done

    set -o xtrace

    docker exec "$rabbitmq_docker_name" rabbitmqctl await_startup
    docker exec "$rabbitmq_docker_name" rabbitmq-diagnostics erlang_version
    docker exec "$rabbitmq_docker_name" rabbitmqctl version

    set -o errexit
}

function get_rabbitmq_id
{
    local rabbitmq_docker_id
    rabbitmq_docker_id="$(docker inspect --format='{{.Id}}' "$rabbitmq_docker_name")"
    echo "[INFO] '$rabbitmq_docker_name' docker id is '$rabbitmq_docker_id'"
    if [[ -v GITHUB_OUTPUT ]]
    then
        if [[ -f $GITHUB_OUTPUT ]]
        then
            echo "[INFO] GITHUB_OUTPUT file: '$GITHUB_OUTPUT'"
        fi
        echo "id=$rabbitmq_docker_id" >> "$GITHUB_OUTPUT"
    fi
}

function install_ca_certificate
{
    set +o errexit
    hostname
    hostname -s
    hostname -f
    openssl version
    openssl version -d
    set -o errexit

    if [[ $GITHUB_ACTIONS == 'true' ]]
    then
        readonly openssl_store_dir='/usr/lib/ssl/certs'
        sudo cp -vf "$GITHUB_WORKSPACE/.ci/certs/ca_certificate.pem" "$openssl_store_dir"
        sudo ln -vsf "$openssl_store_dir/ca_certificate.pem" "$openssl_store_dir/$(openssl x509 -hash -noout -in $openssl_store_dir/ca_certificate.pem).0"
    else
        echo "[WARNING] you must install '$GITHUB_WORKSPACE/.ci/certs/ca_certificate.pem' manually into your trusted root store"
    fi

    openssl s_client -connect localhost:5671 \
        -CAfile "$GITHUB_WORKSPACE/.ci/certs/ca_certificate.pem" \
        -cert "$GITHUB_WORKSPACE/.ci/certs/client_localhost_certificate.pem" \
        -key "$GITHUB_WORKSPACE/.ci/certs/client_localhost_key.pem"
}

docker network create "$docker_network_name" || echo "[INFO] network '$docker_network_name' is already created"

start_toxiproxy

start_rabbitmq

wait_rabbitmq

get_rabbitmq_id

install_ca_certificate
