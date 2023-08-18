from pathlib import Path


def find_idea_dir() -> Path:
    current_dir = Path(__file__).resolve().parent
    while current_dir != Path("/"):
        idea_dir = current_dir / ".idea"
        if idea_dir.is_dir():
            return idea_dir.resolve()
        current_dir = current_dir.parent
    return None


def main():
    print(f"Path to .idea dir: {find_idea_dir()}")


if __name__ == "__main__":
    main()
