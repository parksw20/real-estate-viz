(function(){
  const onReady = (fn)=> document.readyState!=='loading' ? fn() : document.addEventListener('DOMContentLoaded', fn);

  onReady(()=>{
    const t = setInterval(()=>{ if (window.kakao && kakao.maps) { clearInterval(t); initApp(); } }, 40);
  });

  function initApp(){
    // --- Kakao ---
    const map = new kakao.maps.Map(document.getElementById('map'), {
      center:new kakao.maps.LatLng(37.4979,127.0276), level:6
    });
    const clusterer = new kakao.maps.MarkerClusterer({ map, averageCenter:true, minLevel:6 });

    // --- UI refs ---
    const tabs=[...document.querySelectorAll('.tab-btn')];
    const paneData=document.getElementById('pane-data');
    const paneFilters=document.getElementById('pane-filters');

    const addrInput=document.getElementById('addrInput');
    const addrBtn=document.getElementById('addrBtn');

    const gjSelect=document.getElementById('geojsonSelect');

    const selSido=document.getElementById('selSido');
    const selGusi=document.getElementById('selGusi');
    const selDong=document.getElementById('selDong');
    const btnRegionReset=document.getElementById('btnRegionReset');
    const regionListBox=document.getElementById('regionList');

    const htypeChecks=[...document.querySelectorAll('input[name=htype]')];
    const dealSel=document.getElementById('dealSel');
    const priceDual=document.getElementById('priceDual');
    const areaDual=document.getElementById('areaDual');
    const dateDual=document.getElementById('dateDual');
    const priceLbl=document.getElementById('priceLbl');
    const areaLbl=document.getElementById('areaLbl');
    const dateLbl=document.getElementById('dateLbl');
    const resetBtn=document.getElementById('resetBtn');

    const selectedList=document.getElementById('selectedList');

    // --- State ---
    const COLOR={ 매매:'#d81b60', 전세:'#1e88e5', 월세:'#43a047', 기타:'#6d4c41' };
    const DEAL_ORDER={ 매매:0, 전세:1, 월세:2, 기타:3 };

    let rawFeatures=[]; let currentFiltered=[];
    let markers=[]; let infoWindows=[]; let groupIdToMarker=new Map();
    let metaCache={ yms:[] };

    // --- Tabs ---
    function switchTab(name){
      tabs.forEach(b=> b.classList.toggle('active', b.dataset.tab===name));
      paneData.classList.toggle('active', name==='data');
      paneFilters.classList.toggle('active', name==='filters');
    }
    tabs.forEach(b=> b.addEventListener('click', ()=> switchTab(b.dataset.tab)));
    switchTab('filters');

    // --- Helpers ---
    const labelFromFilename = (path)=>{
      const name=String(path).split('/').pop()||'';
      const m=/(\d{6})/.exec(name);
      return m ? `${m[1].slice(0,4)}.${m[1].slice(4,6)}` : name.replace(/\.geojson$/,'');
    };
    const getDealLabel = (p)=>{
      const t=p['거래유형']; const w=Number(p['월세']||0);
      if(t==='매매') return '매매';
      if(t==='전세') return '전세';
      if(t==='월세' || w>0) return '월세';
      return '기타';
    };
    const getPriceMan = (p)=> p['거래유형']==='매매' ? Number(p['거래금액']||0) : Number(p['보증금']||0);
    const getDepositEok = (p)=> { const v=Number(p['보증금']||0); return Number.isFinite(v)? v/10000 : null; };
    const getMonthlyMan = (p)=> { const v=Number(p['월세']||0); return Number.isFinite(v)? v : null; };
    const getAreaPy = (p)=>{ const v=Number(p['전용면적']||0); return Number.isFinite(v)? v/3.3058 : null; };
    const getYyyymm = (p)=>{
      const y=p['년'], m=p['월'];
      if(y!=null && m!=null) return Number(String(y).padStart(4,'0')+String(m).padStart(2,'0'));
      const c=p['계약년월']; return c!=null ? Number(String(c).slice(0,6)) : null;
    };

    function extractRegion(p){
      // 가능한 키를 모두 수용
      const sido = p['시/도'] || p['시도'] || p['시도명'] || '';
      const gusi = p['구/시'] || p['구시'] || p['시군구명'] || p['시군구'] || '';
      const dong = p['법정동'] || p['법정동명'] || p['법정동명칭'] || '';
      if (sido || gusi || dong) return {sido,gusi,dong};
      const addr = p['주소'] || p['지번주소'] || '';
      const m = addr.match(/^([^ ]+?[도|시])\s+([^ ]+?구|[^ ]+?시)\s+([^ ]+?동)/);
      return { sido: m?.[1] || '', gusi: m?.[2] || '', dong: m?.[3] || '' };
    }
    function regionMatch(p){
      const r = extractRegion(p||{});
      if (selSido.value && r.sido !== selSido.value) return false;
      if (selGusi.value && r.gusi !== selGusi.value) return false;
      if (selDong.value && r.dong !== selDong.value) return false;
      return true;
    }

    // --- Dual range ---
    function createDualRange(root, {min=0, max=100, step=1, initMin=min, initMax=max, onInput, format}={}){
      root.innerHTML = '<div class="track"></div><div class="range-fill"></div>';
      const rMin=document.createElement('input'); rMin.type='range'; Object.assign(rMin,{min,max,step,value:initMin});
      const rMax=document.createElement('input'); rMax.type='range'; Object.assign(rMax,{min,max,step,value:initMax});
      rMin.style.zIndex='4'; rMax.style.zIndex='5';
      root.appendChild(rMin); root.appendChild(rMax);
      const fill=root.querySelector('.range-fill');
      const fmt=(v)=> format? format(Number(v)) : Number(v);
      const pct=(v)=> ((v-min)/(max-min))*100;

      let active=null;
      function updateFill(){
        const a=Number(rMin.value), b=Number(rMax.value);
        const left=Math.min(a,b), right=Math.max(a,b);
        fill.style.left=pct(left)+'%'; fill.style.right=(100-pct(right))+'%';
        if(active===rMin){ rMin.style.zIndex='6'; rMax.style.zIndex='5'; }
        else if(active===rMax){ rMax.style.zIndex='6'; rMin.style.zIndex='5'; }
      }
      function clamp(){
        if (+rMin.value > +rMax.value) {
          if (active===rMin) rMax.value=rMin.value; else rMin.value=rMax.value;
        }
        updateFill(); onInput && onInput(fmt(rMin.value), fmt(rMax.value));
      }
      [rMin,rMax].forEach(r=>{
        r.addEventListener('pointerdown', ()=>{ active=r; updateFill(); });
        r.addEventListener('input', (e)=>{ active=e.target; clamp(); });
        r.addEventListener('change', ()=>{ active=null; updateFill(); });
      });
      updateFill();
      return { getMin:()=>+rMin.value, getMax:()=>+rMax.value, set:(a,b)=>{ rMin.value=a; rMax.value=b; updateFill(); onInput && onInput(fmt(rMin.value), fmt(rMax.value)); } };
    }

    // --- GeoJSON loading ---
    async function tryFetch(u){
      try{
        // 한글/공백 경로 안전 처리
        const url = new URL(u, location.href);
        const res = await fetch(url.toString());
        if (!res.ok) throw 0;
        const j = await res.json();
        return Array.isArray(j) ? j : null;
      }catch(_){ return null; }
    }

    async function loadDefaultManifestList(){
      // 요청: "부동산\data\manifest.json"를 자동 로드 (여러 후보를 순차 시도)
      const candidates=[
        '부동산/data/manifest.json',
        '/부동산/data/manifest.json',
        './data/manifest.json',
        '../data/manifest.json'
      ].map(p=> new URL(p, location.href).toString());

      for(const u of candidates){
        const j = await tryFetch(u);
        if (Array.isArray(j)) return { items:j, base:u };
      }
      return { items:[], base:null };
    }
    function toItems(list){
      return list.map(x=>({ path:new URL(x.path, location.href).toString(), label:x.label||labelFromFilename(x.path) }));
    }

    async function loadGeoJSON(url){
      const res=await fetch(url); if(!res.ok) throw new Error('GeoJSON 로드 실패: '+url);
      const gj=await res.json();
      if(!gj || !Array.isArray(gj.features)) throw new Error('GeoJSON 형식 오류');
      rawFeatures=gj.features.filter(f=> f.geometry && f.geometry.type==='Point');
      await afterGeojsonLoaded();
    }

    async function populateFromManifest(){
      // 이제 index.html과 manifest.json이 같은 폴더에 있으므로 단순히 ./manifest.json
      const url = new URL('./manifest.json', location.href).toString();

      try {
        const res = await fetch(url);
        if (!res.ok) throw new Error('manifest fetch error');
        const list = await res.json();
        if (!Array.isArray(list) || !list.length) throw new Error('manifest invalid');

        // GeoJSON 목록 채우기
        const rows = list.map(x => ({
          path: new URL(x.path, url).toString(),
          label: x.label || (String(x.path).match(/(\d{6})/) ?
            `${RegExp.$1.slice(0,4)}.${RegExp.$1.slice(4,6)}` : String(x.path))
        }));

        gjSelect.innerHTML = rows.map(r =>
          `<option value="${r.path}">${r.label}</option>`
        ).join('');

        // ✅ 최신(마지막) 항목 기본 선택 후 GeoJSON 로드
        gjSelect.value = rows[rows.length - 1].path;
        await loadGeoJSON(gjSelect.value);

      } catch (e) {
        console.warn('[manifest] 파일을 읽을 수 없습니다:', e);
        gjSelect.innerHTML = `<option value="">(manifest를 찾지 못했습니다)</option>`;
      }
    }
    
    gjSelect.addEventListener('change', async e=>{
      try{ await loadGeoJSON(e.target.value); }catch(err){ console.warn(err); }
    });

    // --- After GeoJSON ---
    async function afterGeojsonLoaded(){
      const mans=[],pys=[],yms=[];
      rawFeatures.forEach(f=>{
        const p=f.properties||{};
        const m=getPriceMan(p); if(Number.isFinite(m)) mans.push(m);
        const a=getAreaPy(p); if(Number.isFinite(a)) pys.push(a);
        const y=getYyyymm(p); if(Number.isFinite(y)) yms.push(y);
      });
      const mm=a=>a.length?[Math.min(...a),Math.max(...a)]:[0,0];
      const [manMin,manMax]=mm(mans),[pyMin,pyMax]=mm(pys),ymSet=[...new Set(yms)].sort((a,b)=>a-b);
      const eokMin=Math.floor(manMin/10000), eokMax=Math.ceil(manMax/10000)||Math.floor(manMin/10000)+1;
      metaCache={ yms:ymSet };

      const priceCtrl=createDualRange(priceDual,{ min:eokMin, max:eokMax, step:1, initMin:eokMin, initMax:eokMax,
        onInput:(a,b)=>{ priceLbl.textContent=`${a}억 ~ ${b}억`; render(); }});
      const areaCtrl=createDualRange(areaDual,{ min:Math.floor(pyMin), max:Math.ceil(pyMax)||Math.floor(pyMin)+1, step:1,
        initMin:Math.floor(pyMin), initMax:Math.ceil(pyMax)||Math.floor(pyMin)+1,
        onInput:(a,b)=>{ areaLbl.textContent=`${a}평 ~ ${b}평`; render(); }});
      const dateCtrl=createDualRange(dateDual,{ min:0, max:(ymSet.length?ymSet.length-1:0), step:1, initMin:0, initMax:(ymSet.length?ymSet.length-1:0),
        onInput:(a,b)=>{ const s=ymSet[a]||0, e=ymSet[b]||0; const sf=`${String(s).slice(0,4)}.${String(s).slice(4,6)}`, ef=`${String(e).slice(0,4)}.${String(e).slice(4,6)}`; dateLbl.textContent=`${sf} ~ ${ef}`; render(); }});

      priceDual.getRange=priceCtrl; areaDual.getRange=areaCtrl; dateDual.getRange=dateCtrl;

      [dealSel, ...htypeChecks].forEach(el=> el.addEventListener('change', render));
      resetBtn.addEventListener('click', ()=>{
        htypeChecks.forEach(c=> c.checked=true); dealSel.value='';
        priceCtrl.set(eokMin,eokMax); areaCtrl.set(Math.floor(pyMin), Math.ceil(pyMax)||Math.floor(pyMin)+1);
        dateCtrl.set(0, metaCache.yms.length?metaCache.yms.length-1:0);
        selSido.value=''; selGusi.innerHTML='<option value="">구/시</option>'; selDong.innerHTML='<option value="">법정동</option>';
        render(); updateRegionList();
      });

      render();
      buildRegionIndex(rawFeatures);
      updateRegionList();
    }

    function clearMap(){ clusterer.clear(); markers.forEach(m=>m.setMap(null)); markers=[]; infoWindows.forEach(i=>i.close()); infoWindows=[]; groupIdToMarker.clear(); }

    // --- Render (filters → map + list) ---
    function render(){
      const prevCenter=map.getCenter(), prevLevel=map.getLevel();
      clearMap();

      const allowed=new Set(htypeChecks.filter(c=>c.checked).map(c=>c.value));
      const dealPick=dealSel.value;
      const pMin=Number(priceDual.getRange.getMin())*10000, pMax=Number(priceDual.getRange.getMax())*10000;
      const aMin=Number(areaDual.getRange.getMin()), aMax=Number(areaDual.getRange.getMax());
      const dMin=metaCache.yms[Number(dateDual.getRange.getMin())], dMax=metaCache.yms[Number(dateDual.getRange.getMax())];

      const filtered=rawFeatures.filter(ft=>{
        const p=ft.properties||{};
        let ht=p['주택유형']||''; if(ht.includes('연립')) ht='연립다세대'; if(ht.includes('단독')) ht='단독다가구';
        if(!allowed.has(ht)) return false;
        const deal=getDealLabel(p); if(dealPick && deal!==dealPick) return false;
        const man=getPriceMan(p); if(!(man>=pMin && man<=pMax)) return false;
        const py=getAreaPy(p); if(!(py>=aMin && py<=aMax)) return false;
        const ym=getYyyymm(p); if(!(ym>=dMin && ym<=dMax)) return false;
        if(!regionMatch(p)) return false;
        return true;
      });
      currentFiltered = filtered;

      // 그룹(단지/건물)
      const groups=new Map();
      filtered.forEach(ft=>{
        const [lng,lat]=ft.geometry.coordinates; const p=ft.properties||{};
        const name=p['단지명/건물명']||p['건물명']||p['단지명']||p['주소']||'미상';
        const key=name.trim(); const man=getPriceMan(p)||0, deal=getDealLabel(p);
        if(!groups.has(key)) groups.set(key,{name:key,lat,lng,count:0,maxMan:man,deals:new Set([deal])});
        const g=groups.get(key); g.count+=1; g.maxMan=Math.max(g.maxMan,man); g.deals.add(deal);
      });
      const arr=[...groups.values()].sort((a,b)=> b.count-a.count || b.maxMan-a.maxMan || a.name.localeCompare(b.name));

      // 마커
      const bounds=new kakao.maps.LatLngBounds(); const ms=[];
      arr.forEach(g=>{
        const pos=new kakao.maps.LatLng(g.lat,g.lng);
        const color=COLOR[[...g.deals][0]]||'#6d4c41';
        const marker=new kakao.maps.Marker({
          position:pos,
          image:new kakao.maps.MarkerImage(
            'data:image/svg+xml,'+encodeURIComponent(`<svg xmlns="http://www.w3.org/2000/svg" width="22" height="22"><circle cx="11" cy="11" r="8" fill="${color}" stroke="#333" stroke-width="1"/></svg>`),
            new kakao.maps.Size(22,22)
          )
        });
        const iw=new kakao.maps.InfoWindow({content:`<div style="padding:8px 10px; font-size:12px"><div style="font-weight:600">${g.name}</div><div>건수 ${g.count} / 최고가 ${(g.maxMan/10000).toFixed(1).replace(/\.0$/,'')}억</div></div>`});
        kakao.maps.event.addListener(marker,'click',()=>{ iw.open(map, marker); switchTab('data'); map.setLevel(4); map.panTo(marker.getPosition()); marker.setZIndex(999); });
        kakao.maps.event.addListener(marker,'mouseover',()=> iw.open(map, marker));
        kakao.maps.event.addListener(marker,'mouseout',()=> iw.close());
        marker.setMap(map); ms.push(marker); bounds.extend(pos); groupIdToMarker.set(g.name, marker);
      });
      clusterer.addMarkers(ms); markers=ms;

      if(!bounds.isEmpty()){
        const prevEmpty = map.getBounds().isEmpty();
        if(prevEmpty) map.setBounds(bounds,24,24,24,24);
        else { map.setLevel(prevLevel); map.setCenter(prevCenter); }
      }

      // 데이터 탭 카드 (정렬: 거래유형 → 면적↓ → 최신월)
      const sorted = filtered.slice().sort((a,b)=>{
        const da=getDealLabel(a.properties||{}), db=getDealLabel(b.properties||{});
        const od=(DEAL_ORDER[da]??9)-(DEAL_ORDER[db]??9); if(od!==0) return od;
        const aa=Number(a.properties?.['전용면적']||0), ab=Number(b.properties?.['전용면적']||0);
        if(ab!==aa) return ab-aa;
        const ya=getYyyymm(a.properties||{})||0, yb=getYyyymm(b.properties||{})||0; return yb-ya;
      });

      selectedList.innerHTML = sorted.map(ft=>{
        const p=ft.properties||{}; const deal=getDealLabel(p);
        const dealCls = deal==='매매'?'deal-mm': deal==='전세'?'deal-js': deal==='월세'?'deal-ws':'deal-etc';
        const cardCls = deal==='매매'?'card-mm': deal==='전세'?'card-js': deal==='월세'?'card-ws':'card-etc';

        const title=(p['단지명/건물명']||p['건물명']||p['단지명']||p['주소']||'미상').trim();
        const depositEok = getDepositEok(p);  // 억
        const monthlyMan = getMonthlyMan(p);  // 만
        const saleEok = Number(p['거래유형']==='매매' ? (p['거래금액']||0) : 0)/10000;

        const priceLine = (()=>{
          if(deal==='매매') return `<span class="price-strong">${saleEok.toFixed(1).replace(/\.0$/,'')}억</span>`;
          if(deal==='전세') return `<span class="price-strong">${(depositEok??0).toFixed(1).replace(/\.0$/,'')}억</span>`;
          if(deal==='월세') return `<span class="price-strong">${(depositEok??0).toFixed(1).replace(/\.0$/,'')}억 / ${monthlyMan??0}만</span>`;
          // 기타
          const dep = depositEok!=null? `${depositEok.toFixed(1).replace(/\.0$/,'')}억` : '–';
          const mon = monthlyMan!=null? `${monthlyMan}만` : '–';
          return `<span class="price-strong">${dep} / ${mon}</span>`;
        })();

        const yyyymm = (()=>{ const y=p['년'],m=p['월']; if(y!=null&&m!=null) return `${y}.${String(m).padStart(2,'0')}`; const c=p['계약년월']; return c? `${String(c).slice(0,4)}.${String(c).slice(4,6)}` : '-'; })();
        const areaPy = (()=>{ const v=Number(p['전용면적']||0); if(!v) return '–'; const py=v/3.3058; return `${py.toFixed(1).replace(/\.0$/,'')}평`; })();
        const addr=p['주소']||[p['구/시'],p['법정동'],p['도로명'],p['지번']].filter(Boolean).join(' ');

        return `<div class="card ${cardCls}">
          <div style="font-weight:600">${title}</div>
          <div>유형/거래: ${(p['주택유형']||'-')} / <span class="${dealCls}">${deal}</span></div>
          <div>가격: ${priceLine}</div>
          <div>면적: <span class="area-strong">${areaPy}</span></div>
          <div>동/층: ${(p['동']||'-')} / ${(p['층']||'-')}</div>
          <div>주소: ${addr}</div>
          <div>계약: ${yyyymm}</div>
        </div>`;
      }).join('');
    }

    // --- 지역 콤보 & 리스트 ---
    function buildRegionIndex(features){
      const S=new Map();
      features.forEach(ft=>{
        const r=extractRegion(ft.properties||{});
        if(!r.sido) return;
        if(!S.has(r.sido)) S.set(r.sido,new Map());
        const G=S.get(r.sido);
        if(!G.has(r.gusi)) G.set(r.gusi,new Set());
        if(r.dong) G.get(r.gusi).add(r.dong);
      });

      selSido.innerHTML=`<option value="">시/도</option>`+[...S.keys()].sort().map(v=>`<option value="${v}">${v}</option>`).join('');
      selGusi.innerHTML=`<option value="">구/시</option>`;
      selDong.innerHTML=`<option value="">법정동</option>`;

      selSido.onchange=()=>{ const G=S.get(selSido.value)||new Map();
        selGusi.innerHTML=`<option value="">구/시</option>`+[...G.keys()].sort().map(v=>`<option value="${v}">${v}</option>`).join('');
        selDong.innerHTML=`<option value="">법정동</option>`; render(); updateRegionList(); };
      selGusi.onchange=()=>{ const G=S.get(selSido.value)||new Map(); const D=G.get(selGusi.value)||new Set();
        selDong.innerHTML=`<option value="">법정동</option>`+[...D].sort().map(v=>`<option value="${v}">${v}</option>`).join('');
        render(); updateRegionList(); };
      selDong.onchange=()=>{ render(); updateRegionList(); };
      btnRegionReset.onclick=()=>{ selSido.value=''; selGusi.innerHTML=`<option value="">구/시</option>`; selDong.innerHTML=`<option value="">법정동</option>`; render(); updateRegionList(); };
    }

    function updateRegionList(){
      // (6) 법정동까지 선택해야만 결과 표시
      regionListBox.innerHTML = '';
      if (!selSido.value || !selGusi.value || !selDong.value) return;

      const base=currentFiltered||[];
      const groups=new Map();
      base.forEach(ft=>{
        const p=ft.properties||{};
        const name=p['단지명/건물명']||p['건물명']||p['단지명']||p['주소']||'미상';
        const key=name.trim(); const man=getPriceMan(p)||0; const deal=getDealLabel(p);
        if(!groups.has(key)) groups.set(key,{name:key,count:0,maxMan:man,deals:new Set([deal])});
        const g=groups.get(key); g.count+=1; g.maxMan=Math.max(g.maxMan,man); g.deals.add(deal);
      });
      const arr=[...groups.values()].sort((a,b)=> b.count-a.count || b.maxMan-a.maxMan || a.name.localeCompare(b.name));

      regionListBox.innerHTML = `
        <table>
          <thead><tr><th>단지/건물</th><th>건수</th><th>최고가(억)</th></tr></thead>
          <tbody>
            ${arr.map(g=>`<tr class="clickable" data-name="${g.name}">
                <td>${g.name}</td><td style="text-align:right">${g.count}</td><td style="text-align:right">${(g.maxMan/10000).toFixed(1).replace(/\.0$/,'')}</td>
              </tr>`).join('')}
          </tbody>
        </table>`;
      regionListBox.querySelectorAll('tbody tr').forEach(tr=>{
        tr.addEventListener('click', ()=>{
          const name=tr.dataset.name; const m=groupIdToMarker.get(name);
          if(m){ switchTab('data'); map.setLevel(4); map.panTo(m.getPosition()); m.setZIndex(999); }
        });
      });
    }

    // --- “건물명” 찾기(부분 일치로 지도/리스트 이동) ---
    function searchByBuildingName(q){
      q=(q||'').trim(); if(!q) return;
      for (const [name, m] of groupIdToMarker.entries()) {
        if (name.includes(q)) { switchTab('data'); map.setLevel(4); map.panTo(m.getPosition()); m.setZIndex(999); return; }
      }
    }
    addrBtn.addEventListener('click', ()=> searchByBuildingName(addrInput.value));
    addrInput.addEventListener('keydown', e=>{ if(e.key==='Enter') searchByBuildingName(addrInput.value); });

    // --- Boot ---
    (async function(){
      await populateFromManifest(); // (2)(3)
    })();
  }
})();
