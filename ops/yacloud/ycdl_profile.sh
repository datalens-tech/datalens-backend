#!/usr/bin/env bash

if [ -n "${YCDL_ACTIVATED}" ]; then
    echo "already activated"
    return
fi

_YCDL_SHELL_NAME=$(basename $SHELL)

case ${_YCDL_SHELL_NAME} in
    "zsh")
        _YCDL_RED="${fg[red]}"
        _YCDL_GREEN="${fg[green]}"
        _YCDL_NC="${reset_color}"
        _YCDL_PS_RED="%{${_YCDL_RED}%}"
        _YCDL_PS_GREEN="%{${_YCDL_GREEN}%}"
        _YCDL_PS_NC="%{${_YCDL_NC}%}"
        _YCDL_SCRIPT_DIR=$(dirname "$(realpath -s "$0")")
    ;;
    *)
        _YCDL_RED='\033[0;31m'
        _YCDL_GREEN='\033[0;32m'
        _YCDL_NC='\033[0m'
        _YCDL_PS_RED='\[\e[31m\]'
        _YCDL_PS_GREEN='\[\e[32m\]'
        _YCDL_PS_NC='\[\e[0m\]'
        _YCDL_SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"
    ;;
esac

_YCDL_ENV_NAME="$1"
_YCDL_TF_ROOT="${_YCDL_SCRIPT_DIR}/terraform"

_K8S_NAMESPACE="bi-backend"
_K8S_INTEGRATION_TESTS_NAMESPACE="integration-tests"

case "${_YCDL_ENV_NAME}" in
    "yc-preprod")
        _LOCKBOX_DEVOPS_ID='fc3kom16ddopbr5uo4iu'
        _HELM_VALUES=(
            "-f" "${_YCDL_TF_ROOT}/${_YCDL_ENV_NAME}/l2_app/helm_values_bi_backend_manual.yaml"
            "-f" "${_YCDL_TF_ROOT}/${_YCDL_ENV_NAME}/l2_app/helm_values_bi_backend_generated.yaml"
        )
        export TF_VAR_juggler_token=`yc --profile ${_YCDL_ENV_NAME} lockbox payload get --id ${_LOCKBOX_DEVOPS_ID} --key juggler_token`
    ;;
    "israel")
        _LOCKBOX_DEVOPS_ID='bcngat5hm49bmge42mvi'
        _HELM_VALUES=(
            "-f" "${_YCDL_TF_ROOT}/${_YCDL_ENV_NAME}/l2_app/helm_values_bi_backend_manual.yaml"
            "-f" "${_YCDL_TF_ROOT}/${_YCDL_ENV_NAME}/l2_app/helm_values_bi_backend_generated.yaml"
            "-f" "${_YCDL_TF_ROOT}/${_YCDL_ENV_NAME}/l4_sentry_projects/helm_values_bi_backend_generated.yaml"
        )
        export TF_VAR_sentry_token=`yc --profile ${_YCDL_ENV_NAME} lockbox payload get --id ${_LOCKBOX_DEVOPS_ID} --key sentry_token`
    ;;
    "nemax")
        _LOCKBOX_DEVOPS_ID='cd4ll76i58427s6kh7o4'
        _HELM_VALUES=(
            "-f" "${_YCDL_TF_ROOT}/${_YCDL_ENV_NAME}/l2_app/helm_values_bi_backend_manual.yaml"
            "-f" "${_YCDL_TF_ROOT}/${_YCDL_ENV_NAME}/l2_app/helm_values_bi_backend_generated.yaml"
            "-f" "${_YCDL_TF_ROOT}/${_YCDL_ENV_NAME}/l4_sentry_projects/helm_values_bi_backend_generated.yaml"
        )
        export TF_VAR_sentry_token=`yc --profile ${_YCDL_ENV_NAME} lockbox payload get --id ${_LOCKBOX_DEVOPS_ID} --key sentry_token`
    ;;
    *)
        echo -e "${_YCDL_RED}Unknown env_name:${_YCDL_NC} \"${_YCDL_ENV_NAME}\""
        return
    ;;
esac

export KUBECONFIG="${_YCDL_TF_ROOT}/${_YCDL_ENV_NAME}/l1_infra/.kubeconfig_dlback-${_YCDL_ENV_NAME}"
export AWS_ACCESS_KEY=`yc --profile ${_YCDL_ENV_NAME} lockbox payload get --id ${_LOCKBOX_DEVOPS_ID} --key AWS_ACCESS_KEY`
export AWS_SECRET_KEY=`yc --profile ${_YCDL_ENV_NAME} lockbox payload get --id ${_LOCKBOX_DEVOPS_ID} --key AWS_SECRET_KEY`
export YC_PROFILE=${_YCDL_ENV_NAME}

ycdl_iam() {
    export TF_VAR_iam_token=`yc iam create-token --profile ${_YCDL_ENV_NAME}`
}

ycdl_iam

echo -e "${_YCDL_GREEN}Activating YCDL profile for env \"${_YCDL_ENV_NAME}\"${_YCDL_NC}"

YCDL_ENV_PREV_PS1="${PS1}"
YCDL_ENV_PREV_KUBECONFIG="${KUBECONFIG:-}"
YCDL_ACTIVATED="1"

PS1="${_YCDL_PS_RED}(${_YCDL_ENV_NAME})${_YCDL_PS_NC} ${PS1}"

ycdl_reset() {
    if [ -z "${YCDL_ACTIVATED}" ]; then
        echo -e "${_YCDL_RED}Not activated${_YCDL_NC}"
        return
    fi

    PS1="${YCDL_ENV_PREV_PS1}"
    unset YCDL_ACTIVATED
    unset YCDL_ENV_PREV_PS1

    unset AWS_ACCESS_KEY
    unset AWS_SECRET_KEY
    unset TF_VAR_iam_token
    unset TF_VAR_juggler_token
    unset TF_VAR_sentry_token
    unset YC_PROFILE

    if [ -z "${YCDL_ENV_PREV_KUBECONFIG}" ]; then
        unset KUBECONFIG
    else
        export KUBECONFIG=${YCDL_ENV_PREV_KUBECONFIG}
    fi
}

helm_diff() {
    if [ -z "${YCDL_ACTIVATED}" ]; then
        echo -e "${_YCDL_RED}Not activated${_YCDL_NC}"
        return
    fi

    ycdl_iam

    helm diff upgrade -C 5 bi-backend "${_YCDL_SCRIPT_DIR}/helm/bi-backend" \
        "${_HELM_VALUES[@]}" \
        --namespace "${_K8S_NAMESPACE}"
}

helm_upgrade() {
    if [ -z "${YCDL_ACTIVATED}" ]; then
        echo -e "${_YCDL_RED}Not activated${_YCDL_NC}"
        return
    fi

    ycdl_iam

    helm_diff || return 1

    printf "\n\nContinue upgrade? (y/n):"
    read _YCDL_CONFIRM

    if [ "${_YCDL_CONFIRM}" = "y" ]; then
        echo "Going to upgrade"
        helm upgrade bi-backend "${_YCDL_SCRIPT_DIR}/helm/bi-backend" \
            "${_HELM_VALUES[@]}" \
            --namespace "${_K8S_NAMESPACE}"
    else
        echo "Upgrade was cancelled"
    fi
}

helm_template() {
    if [ -z "${YCDL_ACTIVATED}" ]; then
        echo -e "${_YCDL_RED}Not activated${_YCDL_NC}"
        return
    fi

    ycdl_iam

    helm template --debug bi-backend "${_YCDL_SCRIPT_DIR}/helm/bi-backend" \
        "${_HELM_VALUES[@]}" \
        --namespace "${_K8S_NAMESPACE}"
}

helm_test() {
    if [ -z "${YCDL_ACTIVATED}" ]; then
        echo -e "${_YCDL_RED}Not activated${_YCDL_NC}"
        return
    fi

    ycdl_iam

    helm test integration-tests --namespace "${_K8S_INTEGRATION_TESTS_NAMESPACE}"
}

helm_install() {
    if [ -z "${YCDL_ACTIVATED}" ]; then
        echo -e "${_YCDL_RED}Not activated${_YCDL_NC}"
        return
    fi

    ycdl_iam

    helm install bi-backend "${_YCDL_SCRIPT_DIR}/helm/bi-backend" \
        "${_HELM_VALUES[@]}" \
        --namespace "${_K8S_NAMESPACE}"
}
