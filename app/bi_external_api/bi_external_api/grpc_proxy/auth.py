def iam_token_to_grpc_headers(iam_token: str) -> dict[str, str]:
    return {"authorization": f"Bearer {iam_token}"}
