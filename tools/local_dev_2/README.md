# Assuming dev machine was provisoined as before

# Sync to trigger github ci

    0. Get access to https://github.com/datalens-tech/datalens-backend-mirror/  (set ssh key )
    1. Create a checkout on your local host and create a new branch to be used during sync procedure
        e.g.:
            # assuming current dir is backend/tools/local_dev_2
            echo "DIR_GH_CHECKOUT_SYNC=$(realpath ~/ws/datalens_gh_sync/backend)" >> .env
            mkdir -p "$(realpath ~/ws/datalens_gh_sync/..)"
            cd "$(realpath ~/ws/datalens_gh_sync/..)"
            git clone git@github.com:datalens-tech/datalens-backend-mirror.git backend
            cd - && cd "$(realpath ~/ws/datalens_gh_sync/backend)"
            git switch -c "dev-sync-${USER}"
            git push --set-upstream origin "dev-sync-${USER}"
            cd -
    2. Push current state
        make push_gh_sync

# Setup new "local" development based on poetry

    0. Provision VM see: ../dev-machines-provisioning/README.md
    1. Follow instructions from ../local_dev/README.md just instead of tools/local_dev use tools/local_dev_2


# Important points
 - pycharm version 22.1.4 is safest one
 - don't forget to set File mapping (prj root -> /data) in the Project interpreter settings
 - if you have added a new package, cd ../../ops/ci and run `make update_ci_toml`
