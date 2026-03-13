# Arctic Downloader 0.1.5

## Shared Highlights

- Added a new `Flags` section for ComfyUI launch options.
- Added support for starting ComfyUI with `--listen`.
- Improved launch controls so installed add-ons and launch-time flags are handled separately.
- Fixed the PyTorch allocator warning shown during some ComfyUI launches.
- The `Flags` section now lets you toggle `--use-sage-attention`, `--use-flash-attention`, and `--listen` independently from whether the related add-ons are installed.
- The launch UI now uses the ComfyUI launch flags `--use-sage-attention` and `--use-flash-attention` for SageAttention, SageAttention3, and FlashAttention.
- When no special attention backend is enabled at launch, the runtime log now reports `PyTorch attention` instead of `none`.
- Child Python processes now translate deprecated `PYTORCH_CUDA_ALLOC_CONF` usage into `PYTORCH_ALLOC_CONF` and suppress the deprecated variable to avoid PyTorch warnings.

## Linux

- Added `Torch 2.9.1 + ROCm 6.4` as a Linux install option.
- Added AMD GPU detection with automatic ROCm profile selection.
- Added guided ROCm setup for Debian-based, Fedora-based, and Arch-based distros.
- Added ROCm readiness checks and clearer AMD setup guidance in the UI.
- Guided ROCm setup now shows progress directly in the app logs.
- Improved ROCm setup messaging, logging, and responsiveness.
- Reduced repeated sudo prompts during guided ROCm setup.
- Added better handling for group updates and post-install logout/login guidance.
- Hid ROCm setup controls automatically once the system is ready.

## Windows

- Added Windows AMD GPU detection and a Windows ROCm install profile for supported Radeon and Ryzen AI hardware.
- Supported AMD systems now auto-select `Torch 2.9.1 + ROCm SDK 7.2` on Windows.

## Notes

- CUDA-only add-ons remain blocked when an AMD ROCm profile is selected.
- AMD/ROCm support has been implemented for supported Linux distro families and supported Windows Radeon and Ryzen AI hardware, but broader real-world testing is still recommended.
