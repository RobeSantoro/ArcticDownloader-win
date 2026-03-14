# Arctic ComfyUI Helper 0.1.7

## Shared Highlights

- Added more optional ComfyUI startup flags in the `Flags` section:
  - `--lowvram`
  - `--bf16-unet`
  - `--async-offload`
  - `--disable-smart-memory`
- Launch-only startup flags now behave more like users expect and can be changed from both `Install New` and `Manage Existing`.
- Improved Intel/XPU support across the app so Intel GPU systems get a dedicated install path instead of falling back to CUDA-oriented defaults.

## Linux

- Added Linux Intel GPU detection and a new `Torch 2.9.1 + XPU` install profile.
- Added guided Intel setup for Debian-based, Fedora-based, and Arch-based distributions.
- Intel/XPU installs now verify runtime readiness more clearly during preflight.
- Improved the Linux updater flow:
  - fixed version detection so the running binary version is used correctly
  - added visible `Checking...` feedback for the `Check Updates` button
- Clarified Linux preflight messaging so the selected torch profile is shown more clearly in the UI and logs.
- Linux Intel/XPU installs now include Triton XPU support.

## Windows

- Added Windows Intel GPU detection and a new `PyTorch XPU Nightly` install profile.
- Intel GPU systems now auto-select the XPU profile in the ComfyUI installer.
- The Windows launcher now applies Intel XPU environment tweaks automatically when launching an Intel/XPU install.
- Improved the Windows install flow so fresh ComfyUI installs are pinned to the latest release tag instead of immediately appearing outdated.
- Improved post-install update behavior so fresh installs report their version more reliably.
- Improved compatibility after custom-node installs by keeping key Python web dependencies in a healthier state.

## Notes

- CUDA-only add-ons and launch attention flags remain blocked for ROCm and Intel/XPU profiles.
- Linux Intel/XPU support uses the managed XPU install path with guided runtime setup on supported distros.
- Windows Intel/XPU support currently follows the PyTorch nightly Windows XPU path for Intel Arc and related Intel GPU setups.
