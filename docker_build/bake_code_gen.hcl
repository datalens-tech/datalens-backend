target "gen_antlr" {
  contexts = {
    src = "${PROJECT_ROOT}/lib/bi_formula/bi_formula/parser/antlr/"
  }
  dockerfile = "./target_gen_antlr/Dockerfile"
  output     = ["type=local,dest=${PROJECT_ROOT}/lib/bi_formula/bi_formula/parser/antlr/gen"]
}
