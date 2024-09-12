# Phlox

A lightweight tool to mirror Python Package Index (PyPI).

## Installation

Editable installation is recommended, as you may want some customization.

```bash
git clone https://github.com/JLULUG/phlox.git
cd phlox
pip install -e .
```

## Usage

```
usage: python3 -m phlox [options] (--sync | --verify | --delete) [packages ...]

options:
  -h, --help         show this help message and exit
  -V, --version      Dispaly program version
  -v, --verbose      Enable debug logging
  -q, --quiet        Supress info logging
  -w N, --worker N   Concurrent syncing thread
                     Defaults to 4 for sync, 1 for others
  -H, --hash         Calculate hash in file operations
  -d DIR, --dir DIR  Location of local repository
                     Defaults to current directory

command:
  --sync             Sync packages
  --verify           Verify the integrity of local repository
  --delete           Delete specified packages

  packages           Specify packages to sync, verify or delete
                     Defaults to all if not specified
```

## License

This program is developed by Linux User Group of Jilin University, China.

Permissions are granted under conditions specified by the [MIT License](./LICENSE).

This project is named after the flower of the same name.
