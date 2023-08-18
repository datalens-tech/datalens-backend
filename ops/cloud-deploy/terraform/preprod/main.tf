terraform {
  backend "s3" {
    endpoint = "storage.cloud-preprod.yandex.net"
    bucket = "datalens-cloud-preprod-terraform-data"
    region = "us-east-1"
    key = "terraform/terraform.tfstate"

    skip_region_validation = true
    skip_credentials_validation = true
  }
  required_providers {
    ycp = {
      version = "0.14.0"
    }
  }
}

locals {
  folder_id = "aoevv1b69su5144mlro3"
}


module "billing-api-backend" {
  source = "../modules/ig-with-bg"
  instance_group_id = "amckore9qa51ei1rv1sj"
  folder_id = local.folder_id
  name = "billing-api"
  description = "DataLens Billing API (PREPROD)"
}

data "ycp_microcosm_instance_group_instance_group" "billing-worker" {
  instance_group_id = "amci62nn9belufl09oag"
}

module "converter-api-backend" {
  source = "../modules/ig-with-bg"
  instance_group_id = "amcluma7k661vog5rdtv"
  folder_id = local.folder_id
  name = "converter-api"
  description = "DataLens Converter API (PREPROD)"
}

module "dataset-api-backend" {
  source = "../modules/ig-with-bg"
  instance_group_id = "amcgatggnampjfde2ue3"
  folder_id = local.folder_id
  name = "dataset-api"
  description = "DataLens Dataset API (PREPROD)"
}

module "dataset-data-api-backend" {
  source = "../modules/ig-with-bg"
  instance_group_id = "amcqpca11aev311im70k"
  folder_id = local.folder_id
  name = "dataset-data-api"
  description = "DataLens Dataset Data API (PREPROD)"
}

module "dls-api-backend" {
  source = "../modules/ig-with-bg"
  instance_group_id = "amchds7bk8aj647pmca3"
  folder_id = local.folder_id
  name = "dls-api"
  description = "DataLens DLS API (PREPROD)"
}

data "ycp_microcosm_instance_group_instance_group" "dls-tasks" {
  instance_group_id = "amc8u5pf3q0m3b8snbff"
}

module "materializer-api-backend" {
  source = "../modules/ig-with-bg"
  instance_group_id = "amc17j5t64k8dncv1l2r"
  folder_id = local.folder_id
  name = "materializer-api"
  description = "DataLens Materializer API (PREPROD)"
}

module "public-dataset-api-backend" {
  source = "../modules/ig-with-bg"
  instance_group_id = "amcbin2k3v21jaipihe7"
  folder_id = local.folder_id
  name = "public-dataset-api"
  description = "DataLens Public Dataset API (PREPROD)"
}

module "uploads-api-backend"  {
  source = "../modules/ig-with-bg"
  folder_id = local.folder_id
  instance_group_id = "amcccjr1352avpi1ek2k"
  description = "DataLens Public Dataset API (PREPROD)"
  name = "uploads-api"
}
