from pathlib import Path


def install():
    index = Path(__file__).parent / "static" / "index.html"
    text = index.read_text(encoding="utf-8")
    marker = "bitaxe-settings-ui-v2"
    if marker in text:
        return

    css = r'''
<style id="bitaxe-settings-ui-v2">
#settingsDlg{width:min(1120px,96vw);max-height:94vh;padding:0;border:1px solid #343941;border-radius:22px;background:linear-gradient(180deg,#171a1f 0%,#121519 100%);box-shadow:0 28px 90px rgba(0,0,0,.55);overflow:auto}
#settingsDlg::backdrop{background:rgba(4,7,10,.72);backdrop-filter:blur(6px)}
.settings-shell{padding:24px}
.settings-head{display:flex;align-items:flex-start;justify-content:space-between;gap:20px;margin-bottom:18px}
.settings-title-wrap{display:flex;gap:14px;align-items:flex-start}.settings-chip{width:52px;height:52px;border-radius:15px;background:rgba(245,166,35,.10);border:1px solid rgba(245,166,35,.30);display:grid;place-items:center;color:#f5a623;font-size:27px;flex:0 0 auto}.settings-head h2{font-size:30px;line-height:1.05;margin:2px 0 6px}.settings-sub{color:#a7abb4;font-size:14px}.settings-x{border:0;background:transparent;color:#aeb3bd;font-size:26px;padding:2px 8px;font-weight:400}.settings-x:hover{color:#fff;background:#252931}
.settings-top-grid{display:grid;grid-template-columns:1fr 1fr;gap:16px}.settings-card{background:linear-gradient(180deg,rgba(31,35,41,.96),rgba(23,27,32,.96));border:1px solid #333841;border-radius:18px;padding:22px}.settings-card.accent{border-color:rgba(245,166,35,.24)}.settings-card h3{font-size:20px;margin:0}.section-head{display:flex;align-items:center;gap:11px;margin-bottom:4px}.section-icon{color:#f5a623;font-size:22px}.section-desc{margin:0 0 20px 34px;color:#9da2ac;font-size:13px}
.setting-row{display:grid;grid-template-columns:minmax(180px,1fr) minmax(170px,225px);gap:18px;align-items:center;padding:13px 0;border-top:1px solid rgba(255,255,255,.055)}.setting-row:first-of-type{border-top:0}.setting-copy label{font-size:14px;color:#f1f2f4;font-weight:650;margin:0}.setting-copy .hint{margin-top:6px;line-height:1.38;color:#9da2ac;font-size:12px}.setting-control input,.setting-control select{margin:0;background:#101317;border:1px solid #414751;border-radius:10px;padding:11px 12px;font-size:15px;min-height:42px}.setting-control input:focus,.setting-control select:focus{outline:none;border-color:#f5a623;box-shadow:0 0 0 2px rgba(245,166,35,.14)}
.settings-info{display:flex;gap:10px;align-items:flex-start;background:#20242a;border:1px solid #3b414a;border-radius:12px;padding:12px 14px;color:#aeb4bf;font-size:12px;line-height:1.45}.settings-info b{color:#eef0f3}.settings-info.orange{border-color:rgba(245,166,35,.35);background:rgba(245,166,35,.055)}.info-dot{width:20px;height:20px;border:1px solid #7f8793;border-radius:50%;display:grid;place-items:center;flex:0 0 20px;font-size:12px;color:#c7ccd4}.orange .info-dot{border-color:#f5a623;color:#f5a623}
.settings-wide{margin-top:16px}.settings-quad{display:grid;grid-template-columns:1fr 1fr;column-gap:34px;row-gap:2px}.settings-quad .setting-row{grid-template-columns:minmax(180px,1fr) minmax(160px,230px)}
.settings-safety-grid,.settings-behavior-grid{display:grid;grid-template-columns:1fr 1fr;gap:0 34px}.settings-lock{margin:4px 0 8px}.settings-lock label{display:flex;align-items:flex-start;gap:12px;color:#fff;font-size:14px;cursor:pointer}.settings-lock input{width:20px;height:20px;margin:2px 0 0;accent-color:#f5a623;flex:0 0 20px}
.settings-footer{display:flex;justify-content:space-between;align-items:center;gap:16px;margin-top:18px}.settings-footer-right{display:flex;gap:10px}.settings-footer button{min-height:42px;padding:10px 18px}.settings-save{background:linear-gradient(180deg,#ffae2d,#f08c00);color:#111;box-shadow:0 6px 20px rgba(245,166,35,.18)}.settings-reset{background:#1d2127!important;color:#e6e8eb!important;border:1px solid #454b55!important}.settings-cancel{background:#262b32!important;color:#f0f1f2!important}.settings-danger{margin-top:12px;color:#ff9d9d}
@media(max-width:850px){#settingsDlg{width:min(720px,96vw)}.settings-top-grid,.settings-quad,.settings-safety-grid,.settings-behavior-grid{grid-template-columns:1fr}.setting-row,.settings-quad .setting-row{grid-template-columns:1fr}.setting-control{margin-top:2px}.section-desc{margin-left:0}.settings-shell{padding:18px}.settings-head h2{font-size:25px}}
@media(max-width:520px){.settings-head{gap:8px}.settings-chip{width:44px;height:44px}.settings-footer{align-items:stretch;flex-direction:column}.settings-footer-right{display:grid;grid-template-columns:1fr 1fr}.settings-reset{width:100%}}
</style>
'''

    js = r'''
<script id="bitaxe-settings-ui-v2-script">
(function(){
  function field(label,id,type='number',step='',hint='',options=''){
    const control = type==='select'
      ? `<select id="${id}">${options}</select>`
      : `<input id="${id}" type="${type}"${step?` step="${step}"`:''}>`;
    return `<div class="setting-row"><div class="setting-copy"><label for="${id}">${label}</label>${hint?`<div class="hint">${hint}</div>`:''}</div><div class="setting-control">${control}</div></div>`;
  }
  function build(){
    const d=document.getElementById('settingsDlg'); if(!d) return;
    d.innerHTML=`<div class="settings-shell">
      <div class="settings-head">
        <div class="settings-title-wrap"><div class="settings-chip">▣</div><div><h2 id="settingsTitle">Miner Settings</h2><div class="settings-sub">Tune your Bitaxe optimizer for the best performance, efficiency, and thermal safety.</div></div></div>
        <button class="settings-x" type="button" onclick="settingsDlg.close()">×</button>
      </div>

      <div class="settings-top-grid">
        <section class="settings-card accent">
          <div class="section-head"><span class="section-icon">♨</span><h3>Thermal Targets</h3></div><div class="section-desc">Set the target temperatures for optimal performance.</div>
          ${field('ASIC temperature target °C','asic_temp_target','number','0.1','Preferred ASIC temperature. The optimizer reduces F/V when the ASIC runs above this target; the separate hard maximum remains the safety limit.')}
          ${field('VRM temperature target °C','vr_temp_target','number','0.1','Preferred VRM temperature. The optimizer backs down when VRM temperature rises above this target.')}
          <div class="settings-info orange" style="margin-top:18px"><span class="info-dot">i</span><div><b>Shared:</b> ASIC or VRM can trigger AxeOS Auto Fan. Separate/passive: VRM temperature still reduces F/V, but current AxeOS does not expose an independent second-fan command.</div></div>
        </section>

        <section class="settings-card accent">
          <div class="section-head"><span class="section-icon">✣</span><h3>Fan Control</h3></div><div class="section-desc">Configure how the optimizer controls fan speed.</div>
          ${field('Target fan utilization %','fan_target','number','1','Center of the preferred fan operating range. Example: 65% with ±5% tolerance creates a 60–70% target band.')}
          ${field('Fan tolerance ± %','fan_tolerance','number','1','How far above or below the fan target is considered acceptable before the optimizer reacts.')}
          ${field('Fan ceiling %','fan_ceiling','number','1','Maximum allowed fan utilization. The optimizer reduces F/V when this ceiling is exceeded.')}
          ${field('Optimization strategy','efficiency_strategy','select','','Seeks the best balance of efficiency, thermals, and performance.',`<option value="band_efficiency">Efficiency Within Fan Target (default)</option><option value="max_efficiency">Maximum Efficiency</option><option value="performance">Performance Priority</option>`)}
          ${field('VRM cooling topology','vr_cooling','select','','Describes how the VRM is cooled so the optimizer can make appropriate thermal decisions.',`<option value="shared">Shared with ASIC fan</option><option value="separate">Separate VRM fan</option><option value="passive">Passive / uncontrollable</option>`)}
        </section>
      </div>

      <section class="settings-card settings-wide accent">
        <div class="section-head"><span class="section-icon">◴</span><h3>Operating Range</h3></div><div class="section-desc">Set the frequency and core voltage limits for tuning.</div>
        <div class="settings-quad">
          ${field('Minimum frequency MHz','min_frequency','number','','Lowest frequency the optimizer may use during normal tuning.')}
          ${field('Maximum frequency MHz','max_frequency','number','','Highest frequency the optimizer may use during normal tuning.')}
          ${field('Minimum core voltage mV','min_voltage','number','','Lowest core voltage the optimizer may test.')}
          ${field('Maximum core voltage mV','max_voltage','number','','Highest core voltage the optimizer may use.')}
        </div>
      </section>

      <section class="settings-card settings-wide accent">
        <div class="section-head"><span class="section-icon">⬡</span><h3>Safety Limits</h3></div><div class="section-desc">Hard limits to keep your miner safe.</div>
        <div class="settings-info" style="margin-bottom:10px"><span class="info-dot">i</span><div>The optimizer will never intentionally exceed these limits, even if higher values could improve performance.</div></div>
        <div class="settings-safety-grid">
          ${field('ASIC hard maximum °C','max_asic_temp','number','0.1','Absolute ASIC temperature safety limit.')}
          ${field('VRM hard maximum °C','max_vr_temp','number','0.1','Absolute VRM temperature safety limit.')}
          <div id="max_error_pct_wrap">${field('Maximum hardware errors %','max_error_pct','number','0.1','Maximum tolerated hardware error rate before backing down.')}</div>
          <div id="max_reject_pct_wrap">${field('Maximum rejected shares %','max_reject_pct','number','0.1','Maximum tolerated rejected-share rate before backing down.')}</div>
        </div>
      </section>

      <section class="settings-card settings-wide">
        <div class="section-head"><span class="section-icon">⚙</span><h3>Optimizer Behavior</h3></div><div class="section-desc">Advanced tuning cadence and stability controls.</div>
        <div class="settings-info settings-lock"><label for="lock_fv"><input id="lock_fv" type="checkbox"><span><b>Lock F/V</b><br><span class="hint">Hold configured maximum frequency/voltage. Fan settings remain unchanged and the ASIC watchdog stays active.</span></span></label></div>
        <div class="settings-behavior-grid">
          ${field('F/V adjustment step (0.05 = 5%)','op_step','number','0.01','Size of each coupled frequency/voltage tuning move.')}
          ${field('Settle time seconds','settle_seconds','number','','Time to wait after a change before judging stability.')}
          ${field('ASIC deadband °C','temp_deadband','number','0.1','Temperature margin around the ASIC target before reacting.')}
          ${field('VRM deadband °C','vr_temp_deadband','number','0.1','Temperature margin around the VRM target before reacting.')}
          ${field('Fan deadband %','fan_deadband','number','0.5','Fan margin before a target-band correction is triggered.')}
          ${field('Thermal recovery delay seconds','recovery_seconds','number','','Delay before resuming upward tuning after thermal pressure.')}
          ${field('Voltage stability step mV','stability_voltage_step','number','1','Voltage increment used while recovering stability.')}
          ${field('Stable probe time seconds','stability_probe_seconds','number','1','Observation period before accepting a lower-voltage probe.')}
          ${field('Voltage trim decay mV','stability_trim_decay','number','1','Amount of learned stability trim removed during decay.')}
        </div>
      </section>

      <div id="saveErr" class="settings-danger"></div>
      <div class="settings-footer"><button class="settings-reset" type="button" onclick="resetSettingsUi()">↻&nbsp;&nbsp; Reset to defaults</button><div class="settings-footer-right"><button class="settings-cancel" type="button" onclick="settingsDlg.close()">Cancel</button><button class="settings-save" type="button" onclick="saveSettings()">Save settings</button></div></div>
    </div>`;
  }
  window.resetSettingsUi=function(){
    const defaults={asic_temp_target:55,vr_temp_target:65,fan_target:65,fan_tolerance:5,fan_ceiling:75,efficiency_strategy:'band_efficiency',vr_cooling:'shared',min_frequency:400,max_frequency:600,min_voltage:1000,max_voltage:1150,op_step:.05,max_asic_temp:65,max_vr_temp:75,max_error_pct:3,max_reject_pct:1,settle_seconds:30,recovery_seconds:90,temp_deadband:.5,vr_temp_deadband:1,fan_deadband:2,stability_voltage_step:15,stability_probe_seconds:60,stability_trim_decay:5,lock_fv:false};
    for(const [k,v] of Object.entries(defaults)){const el=document.getElementById(k);if(!el)continue;if(el.type==='checkbox')el.checked=!!v;else el.value=v;}
  };
  if(document.readyState==='loading') document.addEventListener('DOMContentLoaded',build); else build();
})();
</script>
'''

    text = text.replace("</head>", css + "\n</head>")
    text = text.replace("</body>", js + "\n</body>")
    index.write_text(text, encoding="utf-8")
