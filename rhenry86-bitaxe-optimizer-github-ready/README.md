# Bitaxe Optimizer

Umbrel Community App Store repo for `rhenry86/bitaxe-optimizer`.

## Install
1. Upload the **contents of this folder** to the root of the GitHub repository `rhenry86/bitaxe-optimizer`.
2. Push/commit to `main`.
3. Open the repository's **Actions** tab and wait for **Publish Docker image** to complete.
4. Ensure the resulting GHCR package is public.
5. In umbrelOS: **App Store → Community App Stores → Add**.
6. Enter: `https://github.com/rhenry86/bitaxe-optimizer`
7. Install **Bitaxe Optimizer**.

## Controller
Frequency and voltage move together along one normalized operating-point axis between the configured min/max values. The controller watches ASIC temperature, fan %, measured J/TH, hardware-error percentage, and rejected-share percentage.

If the operating point reaches minimum F/V and ASIC temperature remains above target, the app enables AxeOS Auto Fan and stays at minimum F/V. It does not force 100% fan. It waits until fan speed falls below the configured target/deadband before resuming coupled F/V optimization.

Efficiency probes are retained only when J/TH improves without violating error/reject limits or materially worsening the thermal/fan objective.
