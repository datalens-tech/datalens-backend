module "nat-instance" {
  count  = var.setup_nat == true ? 1 : 0
  source = "../../nat/v1"

  folder_id              = module.constants.env_data.folder_id
  network_id             = module.constants.env_data.network_id
  locations              = module.subinfra_data.locations
  nat_instance_vm_config = var.nat_config.vm_config
  nat_subnet_cidrs       = var.nat_config.subnet_cidrs
}
