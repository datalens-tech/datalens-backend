target "gen_antlr" {
  contexts = {
    src = "${DL_B_PROJECT_ROOT}/lib/dl_formula/dl_formula/parser/antlr/"
  }
  dockerfile = "./target_gen_antlr/Dockerfile"
  output     = ["type=local,dest=${DL_B_PROJECT_ROOT}/lib/dl_formula/dl_formula/parser/antlr/gen"]
}
