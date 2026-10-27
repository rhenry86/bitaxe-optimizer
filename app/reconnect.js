(function(){
  let reconnectMinerId = null;

  function ensureReconnectControl(){
    if(document.getElementById('auto_reconnect')) return;
    const lock = document.getElementById('lock_fv');
    if(!lock) return;
    const box = document.createElement('div');
    box.className='full';
    box.style='background:#24262b;border:1px solid #454850;border-radius:10px;padding:14px';
    box.innerHTML=`<label for="auto_reconnect" style="display:flex;align-items:center;gap:12px;font-size:14px;color:#fff;cursor:pointer">
      <input id="auto_reconnect" type="checkbox" checked style="display:inline-block;width:20px;height:20px;margin:0;flex:0 0 20px;accent-color:#f5a623">
      <span><b>Auto reconnect & resume optimization</b><br><span class="hint">Recommended. If the Bitaxe disconnects, the app restarts, or the optimizer task crashes, automatically restore optimizer control. Pressing Pause still keeps the miner paused.</span></span>
    </label>`;
    const parentBlock = lock.closest('.full');
    if(parentBlock && parentBlock.parentNode) parentBlock.parentNode.insertBefore(box,parentBlock);
  }

  const originalOpenSettings = window.openSettings;
  window.openSettings = async function(id,name){
    reconnectMinerId=id;
    ensureReconnectControl();
    await originalOpenSettings(id,name);
    try{
      const s=await api('/api/miners/'+id+'/reconnect-state');
      const el=document.getElementById('auto_reconnect');
      if(el) el.checked=s.enabled!==false;
    }catch(_){
      const el=document.getElementById('auto_reconnect');
      if(el) el.checked=true;
    }
  };

  const originalSaveSettings = window.saveSettings;
  window.saveSettings = async function(){
    const id=reconnectMinerId;
    const enabled=document.getElementById('auto_reconnect')?.checked!==false;
    await originalSaveSettings();
    if(id!=null){
      try{
        await api('/api/miners/'+id+'/reconnect-state',{
          method:'PUT',headers:{'content-type':'application/json'},body:JSON.stringify({enabled})
        });
      }catch(err){ console.error('Failed to save auto reconnect setting',err); }
    }
  };

  window.go = async function(id){
    await api('/api/miners/'+id+'/managed-optimize',{method:'POST'});
    load();
  };

  window.stop = async function(id){
    await api('/api/miners/'+id+'/managed-stop',{method:'POST'});
    load();
  };

  ensureReconnectControl();
})();
