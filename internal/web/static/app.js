(() => {
  const apiBase = window.__HDCASTER_API_BASE__ || window.__HDCASER_API_BASE__ || "/api/v1";

  const state = {
    loading: true,
    connected: false,
    authenticated: false,
    authConfig: null,
    authSession: null,
    adminProfile: null,
    authSettings: null,
    authMessage: new URLSearchParams(window.location.search).get("auth_error") || "",
    lastUpdated: null,
    overview: null,
    sources: [],
    mounts: [],
    relays: [],
    auditEvents: [],
    users: [],
    userPageResult: { items: [], total: 0, page: 1, page_size: 25 },
    relayPageResult: { items: [], total: 0, page: 1, page_size: 25 },
    blocks: [],
    limits: null,
    mountAdminPageResult: { items: [], total: 0, page: 1, page_size: 25 },
    selectedMountId: null,
    mountDetailData: null,
    mountHistory: [],
    activePage: "home",
    message: "准备就绪 Ready",
    lang: localStorage.getItem("hdcaster_lang") || "zh",
  };

  let homeRefreshTimer = null;

  // ── i18n labels ────────────────────────────────────────────────────────────
  const i18n = {
    "zh": {
      "tab.status": "状态",
      "tab.config": "配置",
      "tab.system": "系统",
      "topbar.logout": "退出登录",
      "topbar.refresh": "刷新数据",
      "topbar.backup": "备份 SQLite",
      "auth.title": "登录后台",
      "auth.username": "用户名",
      "auth.password": "密码",
      "auth.loginBtn": "用户名密码登录",
      "auth.or": "或",
      "notice.kicker": "安全提示",
      "notice.title": "默认管理员仍在使用初始凭据",
      "notice.btn": "前往系统",
      "notice.desc": "首次运行且未启用 OIDC 时默认允许本地密码访问。<br>为了系统安全，请尽快使用此账号进入系统修改密码。",
      "status.overview.kicker": "概览仪表盘",
      "status.overview.title": "运行态总览",
      "status.sources.kicker": "在线状态",
      "status.sources.title": "在线数据源",
      "status.mounts.kicker": "在线状态",
      "status.mounts.title": "在线挂载点",
      "status.relay.kicker": "中继状态",
      "status.relay.title": "中继运行态",
      "status.rejects.kicker": "中继告警",
      "status.rejects.title": "最近拒绝事件",
      "status.audit.kicker": "系统审计",
      "status.audit.title": "最近审计事件",
      "status.mountDetail.kicker": "挂载点详情",
      "status.mountDetail.title": "选中挂载点的元数据与 RTCM 信息",
      "config.users.kicker": "用户管理",
      "config.users.title": "账号与权限",
      "config.users.formTitle": "新增或更新用户",
      "config.users.role": "用户类型",
      "config.users.username": "用户名",
      "config.users.password": "密码",
      "config.users.mounts": "挂载点权限",
      "config.users.note": "备注",
      "config.users.save": "保存用户",
      "config.users.listTitle": "所有用户",
      "config.mounts.kicker": "挂载点配置",
      "config.mounts.title": "新增或更新挂载点元数据",
      "config.mounts.formTitle": "挂载点编辑",
      "config.mounts.name": "挂载点名称",
      "config.mounts.lat": "纬度",
      "config.mounts.lon": "经度",
      "config.mounts.decode": "解码候选",
      "config.mounts.desc": "描述",
      "config.mounts.constellations": "支持星系",
      "config.mounts.rtcm": "RTCM 消息类型",
      "config.mounts.save": "保存挂载点",
      "config.mounts.listTitle": "所有挂载点",
      "config.relay.kicker": "中继配置",
      "config.relay.title": "新增或更新中继",
      "config.relay.formTitle": "中继编辑",
      "config.relay.name": "中继名称",
      "config.relay.enabled": "启用",
      "config.relay.localMount": "本地挂载点",
      "config.relay.upstreamHost": "上游主机",
      "config.relay.upstreamPort": "上游端口",
      "config.relay.upstreamMount": "上游挂载点",
      "config.relay.upstreamUser": "上游用户名",
      "config.relay.upstreamPass": "上游密码",
      "config.relay.clusterRadius": "聚类半径 (km)",
      "config.relay.clusterSlots": "聚类槽位",
      "config.relay.ntripVer": "NTRIP 版本",
      "config.relay.accountPool": "账号池",
      "config.relay.staticGga": "静态 GGA",
      "config.relay.ggaInterval": "GGA 间隔秒数",
      "config.relay.desc": "描述",
      "config.relay.save": "保存中继",
      "config.relay.listTitle": "所有中继",
      "system.lang.kicker": "界面语言",
      "system.lang.title": "语言设置",
      "system.lang.choose": "选择显示语言",
      "system.lang.label": "语言",
      "system.lang.desc": "切换后界面标签将以所选语言显示。",
      "system.auth.kicker": "认证管理",
      "system.auth.title": "当前管理员与登录方式",
      "system.auth.adminTitle": "当前管理员",
      "system.auth.localLogin": "启用用户名密码登录",
      "system.auth.newPass": "新密码",
      "system.auth.saveAdmin": "保存管理员",
      "system.auth.loginMethod": "登录方式",
      "system.auth.oidcEnabled": "启用 OIDC",
      "system.auth.oidcProvider": "OIDC 提供方",
      "system.auth.saveLogin": "保存登录方式",
      "system.limits.kicker": "运行配额",
      "system.limits.title": "接入上限与资源控制",
      "system.limits.editTitle": "编辑限制",
      "system.limits.maxClients": "最大 Client 数量",
      "system.limits.maxSources": "最大 Source 数量",
      "system.limits.maxPending": "最大 Pending 数量",
      "system.limits.maxConn": "最大并发连接",
      "system.limits.save": "保存限制",
      "system.limits.currentTitle": "当前限制",
      "system.block.kicker": "访问控制",
      "system.block.title": "IP 封锁名单",
      "system.block.addTitle": "添加封锁规则",
      "system.block.expires": "过期时间",
      "system.block.reason": "原因",
      "system.block.addBtn": "加入封锁列表",
      "system.block.listTitle": "当前封锁列表",
      "col.status": "状态",
      "col.relay": "中继",
      "col.upstream": "上游",
      "col.accountPool": "账号池",
      "col.runtime": "运行态",
      "col.cluster": "聚类",
      "col.type": "类型",
      "col.username": "用户名",
      "col.permissions": "权限",
      "col.actions": "操作",
      "col.mountpoint": "挂载点",
      "col.address": "地址",
      "col.reason": "原因",
      "col.expires": "到期时间",
      "pager.prev": "上一页",
      "pager.next": "下一页",
      "opt.yes": "是",
      "opt.no": "否",
      "opt.enable": "启用",
      "opt.disable": "禁用",
    },
    "en": {
      "tab.status": "Status",
      "tab.config": "Config",
      "tab.system": "System",
      "topbar.logout": "Logout",
      "topbar.refresh": "Refresh",
      "topbar.backup": "Backup SQLite",
      "auth.title": "Admin Login",
      "auth.username": "Username",
      "auth.password": "Password",
      "auth.loginBtn": "Login",
      "auth.or": "Or",
      "notice.kicker": "Security Notice",
      "notice.title": "Default admin is using initial credentials",
      "notice.btn": "Go to System",
      "notice.desc": "On first run without OIDC, local password login is enabled by default.<br>Please change the admin credentials promptly.",
      "status.overview.kicker": "Dashboard Overview",
      "status.overview.title": "Runtime Summary",
      "status.sources.kicker": "Online Status",
      "status.sources.title": "Online Sources",
      "status.mounts.kicker": "Online Status",
      "status.mounts.title": "Online Mountpoints",
      "status.relay.kicker": "Relay Status",
      "status.relay.title": "Relay Runtime",
      "status.rejects.kicker": "Relay Alerts",
      "status.rejects.title": "Recent Rejection Events",
      "status.audit.kicker": "System Audit",
      "status.audit.title": "Recent Audit Events",
      "status.mountDetail.kicker": "Mountpoint Detail",
      "status.mountDetail.title": "Selected Mountpoint Metadata & RTCM Info",
      "config.users.kicker": "User Management",
      "config.users.title": "Accounts & Permissions",
      "config.users.formTitle": "Add / Update User",
      "config.users.role": "User Type",
      "config.users.username": "Username",
      "config.users.password": "Password",
      "config.users.mounts": "Mountpoint Permissions",
      "config.users.note": "Note",
      "config.users.save": "Save User",
      "config.users.listTitle": "All Users",
      "config.mounts.kicker": "Mountpoint Configuration",
      "config.mounts.title": "Add / Update Mountpoint Metadata",
      "config.mounts.formTitle": "Edit Mountpoint",
      "config.mounts.name": "Mountpoint Name",
      "config.mounts.lat": "Latitude",
      "config.mounts.lon": "Longitude",
      "config.mounts.decode": "Decode Candidate",
      "config.mounts.desc": "Description",
      "config.mounts.constellations": "Supported Constellations",
      "config.mounts.rtcm": "RTCM Message Types",
      "config.mounts.save": "Save Mountpoint",
      "config.mounts.listTitle": "All Mountpoints",
      "config.relay.kicker": "Relay Configuration",
      "config.relay.title": "Add / Update Relay",
      "config.relay.formTitle": "Edit Relay",
      "config.relay.name": "Relay Name",
      "config.relay.enabled": "Enable",
      "config.relay.localMount": "Local Mountpoint",
      "config.relay.upstreamHost": "Upstream Host",
      "config.relay.upstreamPort": "Upstream Port",
      "config.relay.upstreamMount": "Upstream Mountpoint",
      "config.relay.upstreamUser": "Upstream Username",
      "config.relay.upstreamPass": "Upstream Password",
      "config.relay.clusterRadius": "Cluster Radius (km)",
      "config.relay.clusterSlots": "Cluster Slots",
      "config.relay.ntripVer": "NTRIP Version",
      "config.relay.accountPool": "Account Pool",
      "config.relay.staticGga": "Static GGA",
      "config.relay.ggaInterval": "GGA Interval (s)",
      "config.relay.desc": "Description",
      "config.relay.save": "Save Relay",
      "config.relay.listTitle": "All Relays",
      "system.lang.kicker": "Interface Language",
      "system.lang.title": "Language Settings",
      "system.lang.choose": "Choose Display Language",
      "system.lang.label": "Language",
      "system.lang.desc": "Switching will update interface labels to the selected language.",
      "system.auth.kicker": "Authentication Management",
      "system.auth.title": "Admin Profile & Login Method",
      "system.auth.adminTitle": "Current Admin",
      "system.auth.localLogin": "Enable Password Login",
      "system.auth.newPass": "New Password",
      "system.auth.saveAdmin": "Save Admin",
      "system.auth.loginMethod": "Login Method",
      "system.auth.oidcEnabled": "Enable OIDC",
      "system.auth.oidcProvider": "OIDC Provider",
      "system.auth.saveLogin": "Save Login Method",
      "system.limits.kicker": "Resource Quotas",
      "system.limits.title": "Connection Limits & Resource Control",
      "system.limits.editTitle": "Edit Limits",
      "system.limits.maxClients": "Max Clients",
      "system.limits.maxSources": "Max Sources",
      "system.limits.maxPending": "Max Pending",
      "system.limits.maxConn": "Max Connections",
      "system.limits.save": "Save Limits",
      "system.limits.currentTitle": "Current Limits",
      "system.block.kicker": "Access Control",
      "system.block.title": "IP Block List",
      "system.block.addTitle": "Add Block Rule",
      "system.block.expires": "Expires At",
      "system.block.reason": "Reason",
      "system.block.addBtn": "Add to Block List",
      "system.block.listTitle": "Current Block List",
      "col.status": "State",
      "col.relay": "Relay",
      "col.upstream": "Upstream",
      "col.accountPool": "Account Pool",
      "col.runtime": "Runtime",
      "col.cluster": "Cluster",
      "col.type": "Type",
      "col.username": "Username",
      "col.permissions": "Permissions",
      "col.actions": "Actions",
      "col.mountpoint": "Mountpoint",
      "col.address": "Address",
      "col.reason": "Reason",
      "col.expires": "Expires",
      "pager.prev": "Prev",
      "pager.next": "Next",
      "opt.yes": "Yes",
      "opt.no": "No",
      "opt.enable": "Enable",
      "opt.disable": "Disable",
    },
  };

  function t(key) {
    return (i18n[state.lang] || i18n["zh"])[key] || (i18n["zh"][key] || key);
  }
  function applyI18n() {
    document.querySelectorAll("[data-i18n]").forEach((el) => {
      const key = el.getAttribute("data-i18n");
      const text = t(key);
      if (text) el.innerHTML = text; // 使用 innerHTML 允许诸如 <br> 的排版
    });
    // Update placeholders
    const ph = state.lang === "en" ? {
      userSearch: "Search username / type / permissions",
      mountSearch: "Search mountpoint name",
      relaySearch: "Search relay / local mount / upstream",
      userPassword: "Required for new users; leave blank to keep current",
      userNote: "Optional description",
      mountDesc: "Station purpose, region, features, etc.",
      relayDesc: "Upstream source, region, purpose",
      relayPool: "name,username,password[,enabled][,expiresAtRFC3339]",
      relayGga: "$GPGGA,...",
      blockIp: "203.0.113.10 / 203.0.113.0/24",
      blockExpires: "Optional, e.g. 2026-04-01T00:00:00Z",
      blockReason: "Flooding, brute-force, etc.",
      relayUpstreamPass: "Leave blank to keep or use anonymous",
    } : {
      userSearch: "搜索用户名、类型、权限",
      mountSearch: "搜索挂载点名称",
      relaySearch: "搜索中继、本地挂载点、上游地址",
      userPassword: "新增用户必填；编辑留空则不修改",
      userNote: "可选说明",
      mountDesc: "基站用途、区域、特色等",
      relayDesc: "上游来源、地区、用途",
      relayPool: "name,username,password[,enabled][,expiresAtRFC3339]",
      relayGga: "$GPGGA,...",
      blockIp: "203.0.113.10 / 203.0.113.0/24",
      blockExpires: "可选，例如 2026-04-01T00:00:00Z",
      blockReason: "刷流量、暴力连接等",
      relayUpstreamPass: "留空则保持原值或匿名",
    };
    const setPh = (id, key) => { const el = document.getElementById(id); if (el) el.placeholder = ph[key] || ""; };
    setPh("userSearch", "userSearch");
    setPh("mountSearch", "mountSearch");
    setPh("relaySearch", "relaySearch");
    const uf = document.getElementById("userForm");
    if (uf) {
      uf.password.placeholder = ph.userPassword;
      uf.note.placeholder = ph.userNote;
    }
    const mf = document.getElementById("mountForm");
    if (mf) mf.description.placeholder = ph.mountDesc;
    const rf = document.getElementById("relayForm");
    if (rf) {
      rf.description.placeholder = ph.relayDesc;
      rf.accountPool.placeholder = ph.relayPool;
      rf.ggaSentence.placeholder = ph.relayGga;
      rf.password.placeholder = ph.relayUpstreamPass;
    }
    const bf = document.getElementById("blockForm");
    if (bf) {
      bf.ip.placeholder = ph.blockIp;
      bf.expiresAt.placeholder = ph.blockExpires;
      bf.reason.placeholder = ph.blockReason;
    }
    // Update html lang attribute
    document.documentElement.lang = state.lang === "en" ? "en" : "zh-CN";
    // Sync langSelect if present
    const sel = document.getElementById("langSelect");
    if (sel && sel.value !== state.lang) sel.value = state.lang;
  }

  const fallbackData = {
    overview: { activeSources: 0, activeMounts: 0, connectedClients: 0, blockedIPs: 0, throughputKbps: 0, decodeCandidates: 0, activeRelays: 0, relayErrors: 0 },
    sources: [],
    mounts: [],
    relays: [],
    auditEvents: [],
    users: [],
    blocks: [],
    limits: { maxClients: 0, maxSources: 0, maxPending: 0, maxConnections: 0 },
  };

  const els = {
    authScreen: document.getElementById("authScreen"),
    appShell: document.getElementById("appShell"),
    authMessage: document.getElementById("authMessage"),
    loginForm: document.getElementById("loginForm"),
    passwordLoginBtn: document.getElementById("passwordLoginBtn"),
    authDivider: document.getElementById("authDivider"),
    oidcLoginBtn: document.getElementById("oidcLoginBtn"),
    logoutBtn: document.getElementById("logoutBtn"),
    refreshBtn: document.getElementById("refreshBtn"),
    backupSqliteBtn: document.getElementById("backupSqliteBtn"),
    tabHome: document.getElementById("tabHome"),
    tabUsers: document.getElementById("tabUsers"),
    tabSettings: document.getElementById("tabSettings"),
    securityNotice: document.getElementById("securityNotice"),
    openSettingsFromNotice: document.getElementById("openSettingsFromNotice"),
    pageHome: document.getElementById("pageHome"),
    pageUsers: document.getElementById("pageUsers"),
    pageSettings: document.getElementById("pageSettings"),
    connectionStatus: document.getElementById("connectionStatus"),
    lastUpdated: document.getElementById("lastUpdated"),
    overviewCards: document.getElementById("overviewCards"),
    miniMetrics: document.getElementById("miniMetrics"),
    onlineSources: document.getElementById("onlineSources"),
    mountpoints: document.getElementById("mountpoints"),
    userTable: document.getElementById("userTable"),
    userSearch: document.getElementById("userSearch"),
    userCount: document.getElementById("userCount"),
    userPrevBtn: document.getElementById("userPrevBtn"),
    userNextBtn: document.getElementById("userNextBtn"),
    userPageInfo: document.getElementById("userPageInfo"),
    blockTable: document.getElementById("blockTable"),
    quotaGrid: document.getElementById("quotaGrid"),
    mountDetail: document.getElementById("mountDetail"),
    mountForm: document.getElementById("mountForm"),
    mountConfigList: document.getElementById("mountConfigList"),
    relayConfigList: document.getElementById("relayConfigList"),
    mountSearch: document.getElementById("mountSearch"),
    mountCount: document.getElementById("mountCount"),
    mountPrevBtn: document.getElementById("mountPrevBtn"),
    mountNextBtn: document.getElementById("mountNextBtn"),
    mountPageInfo: document.getElementById("mountPageInfo"),
    relayTable: document.getElementById("relayTable"),
    relaySearch: document.getElementById("relaySearch"),
    relayCount: document.getElementById("relayCount"),
    relayPrevBtn: document.getElementById("relayPrevBtn"),
    relayNextBtn: document.getElementById("relayNextBtn"),
    relayPageInfo: document.getElementById("relayPageInfo"),
    relayForm: document.getElementById("relayForm"),
    relayRejects: document.getElementById("relayRejects"),
    auditEvents: document.getElementById("auditEvents"),
    userForm: document.getElementById("userForm"),
    blockForm: document.getElementById("blockForm"),
    limitsForm: document.getElementById("limitsForm"),
    adminProfileForm: document.getElementById("adminProfileForm"),
    authSettingsForm: document.getElementById("authSettingsForm"),
  };

  const api = {
    async request(path, options = {}) {
      const response = await fetch(`${apiBase}${path}`, {
        headers: {
          "Content-Type": "application/json",
          ...(options.headers || {}),
        },
        credentials: "same-origin",
        ...options,
      });

      if (response.status === 401) {
        const error = new Error(`API ${path} failed: 401`);
        error.code = 401;
        throw error;
      }
      const text = await response.text();
      if (!response.ok) {
        let message = `API ${path} failed: ${response.status}`;
        if (text) {
          try {
            const payload = JSON.parse(text);
            message = payload.error || message;
          } catch {
            message = text;
          }
        }
        throw new Error(message);
      }
      return text ? JSON.parse(text) : null;
    },
    authConfig() {
      return this.request("/auth/config");
    },
    authSession() {
      return this.request("/auth/session");
    },
    adminProfile() {
      return this.request("/settings/admin");
    },
    authSettings() {
      return this.request("/settings/auth");
    },
    login(payload) {
      return this.request("/auth/login", { method: "POST", body: JSON.stringify(payload) });
    },
    logout() {
      return this.request("/auth/logout", { method: "POST" });
    },
    startOIDC() {
      window.location.href = `${apiBase}/auth/oidc/start`;
    },
    overview() { return this.request("/overview"); },
    sources() { return this.request("/sources/online"); },
    mounts() { return this.request("/mounts"); },
    relays() { return this.request("/relays"); },
    audit(limit = 10) { return this.request(`/audit?limit=${limit}`); },
    users() { return this.request("/users"); },
    usersPage(page, pageSize, query) { return this.request(`/users?page=${page}&page_size=${pageSize}&q=${encodeURIComponent(query || "")}`); },
    relaysPage(page, pageSize, query) { return this.request(`/relays?page=${page}&page_size=${pageSize}&q=${encodeURIComponent(query || "")}`); },
    blocks() { return this.request("/blocks"); },
    limits() { return this.request("/limits"); },
    saveUser(payload) { return this.request("/users", { method: "POST", body: JSON.stringify(payload) }); },
    saveRelay(payload) { return this.request("/relays", { method: "POST", body: JSON.stringify(payload) }); },
    deleteUser(type, username) { return this.request(`/users/${encodeURIComponent(type)}/${encodeURIComponent(username)}`, { method: "DELETE" }); },
    setUserEnabled(type, username, enabled) { return this.request(`/users/${encodeURIComponent(type)}/${encodeURIComponent(username)}/enabled`, { method: "PUT", body: JSON.stringify({ enabled }) }); },
    deleteRelay(name) { return this.request(`/relays/${encodeURIComponent(name)}`, { method: "DELETE" }); },
    setRelayEnabled(name, enabled) { return this.request(`/relays/${encodeURIComponent(name)}/enabled`, { method: "PUT", body: JSON.stringify({ enabled }) }); },
    saveBlock(payload) { return this.request("/blocks", { method: "POST", body: JSON.stringify(payload) }); },
    deleteBlock(ip) { return this.request(`/blocks/${encodeURIComponent(ip)}`, { method: "DELETE" }); },
    saveLimits(payload) { return this.request("/limits", { method: "PUT", body: JSON.stringify(payload) }); },
    saveAdminProfile(payload) { return this.request("/settings/admin", { method: "PUT", body: JSON.stringify(payload) }); },
    saveAuthSettings(payload) { return this.request("/settings/auth", { method: "PUT", body: JSON.stringify(payload) }); },
    mountDetail(id) { return this.request(`/mounts/${encodeURIComponent(id)}`); },
    mountHistory(id) { return this.request(`/mounts/${encodeURIComponent(id)}/history`); },
    saveMount(payload) { return this.request("/mounts", { method: "POST", body: JSON.stringify(payload) }); },
    mountsPage(page, pageSize, query) { return this.request(`/mounts?page=${page}&page_size=${pageSize}&q=${encodeURIComponent(query || "")}`); },
    deleteMount(id) { return this.request(`/mounts/${encodeURIComponent(id)}`, { method: "DELETE" }); },
    setMountEnabled(id, enabled) { return this.request(`/mounts/${encodeURIComponent(id)}/enabled`, { method: "PUT", body: JSON.stringify({ enabled }) }); },
    download(path) { window.location.href = `${apiBase}${path}`; },
  };

  function setState(patch) {
    Object.assign(state, patch);
    render();
  }

  function lv(zh, en) { return state.lang === "en" ? en : zh; }

  function formatValue(value) {
    if (value === null || value === undefined || value === "") return lv("未设置", "N/A");
    return value;
  }

  function formatTime(value) {
    if (!value || value === "永久") return value === "永久" ? lv("永久", "Permanent") : lv("未设置", "N/A");
    const locale = state.lang === "en" ? "en-US" : "zh-CN";
    return new Intl.DateTimeFormat(locale, { dateStyle: "medium", timeStyle: "short" }).format(new Date(value));
  }

  function formatGeoPoint(point) {
    if (!point) return lv("未设置", "N/A");
    const lat = Number(point.latitude);
    const lon = Number(point.longitude);
    if (!Number.isFinite(lat) || !Number.isFinite(lon)) return lv("未设置", "N/A");
    const parts = [lat.toFixed(6), lon.toFixed(6)];
    if (Number.isFinite(Number(point.altitude))) {
      parts.push(`${Number(point.altitude).toFixed(2)}m`);
    }
    return parts.join(", ");
  }

  function formatReferenceStation(reference) {
    if (!reference) return [];
    const rows = [];
    if (reference.station_id) rows.push([lv("参考站 ID", "Station ID"), reference.station_id]);
    if (reference.itrf_year || reference.itrf_year === 0) rows.push([lv("ITRF 年份", "ITRF Year"), reference.itrf_year]);
    if (reference.antenna_descriptor) rows.push([lv("天线型号", "Antenna Type"), reference.antenna_descriptor]);
    if (reference.antenna_serial) rows.push([lv("天线序列号", "Antenna Serial"), reference.antenna_serial]);
    if (reference.antenna_setup_id || reference.antenna_setup_id === 0) rows.push([lv("天线 Setup ID", "Antenna Setup ID"), reference.antenna_setup_id]);
    if (reference.receiver_type) rows.push([lv("接收机型号", "Receiver Type"), reference.receiver_type]);
    if (reference.receiver_firmware) rows.push([lv("接收机固件", "Receiver Firmware"), reference.receiver_firmware]);
    if (reference.receiver_serial) rows.push([lv("接收机序列号", "Receiver Serial"), reference.receiver_serial]);
    const flags = [
      reference.gps_indicator ? "GPS" : null,
      reference.glonass_indicator ? "GLONASS" : null,
      reference.galileo_indicator ? "Galileo" : null,
      reference.reference_station_indicator ? "Reference" : null,
      reference.single_receiver_oscillator ? "Single Osc" : null,
    ].filter(Boolean);
    if (flags.length) rows.push([lv("参考站旗标", "Station Flags"), flags.join(", ")]);
    if (Number.isFinite(Number(reference.quarter_cycle_indicator))) rows.push(["Quarter Cycle", reference.quarter_cycle_indicator]);
    if (Number.isFinite(Number(reference.antenna_height_meters))) {
      rows.push([lv("天线高", "Antenna Height"), `${Number(reference.antenna_height_meters).toFixed(4)} m`]);
    }
    return rows;
  }

  function formatMSMClasses(runtime) {
    const items = runtime?.msmClasses || [];
    return items.length ? items.join(", ") : lv("暂无", "None");
  }

  function formatMSMFamilies(runtime) {
    const families = runtime?.msmFamilies || [];
    if (!families.length) return lv("暂无", "None");
    return families.map((family) => `${family.system}: ${(family.msmClasses || []).join("/")}`).join(" · ");
  }

  function render() {
    const authReady = !!state.authConfig;
    const localEnabled = !!state.authConfig?.local?.enabled;
    const oidcEnabled = !!state.authConfig?.oidc?.enabled;

    applyI18n();

    els.authScreen.style.display = state.authenticated ? "none" : "grid";
    els.appShell.classList.toggle("shell-hidden", !state.authenticated);
    els.loginForm.style.display = localEnabled ? "grid" : "none";
    els.authDivider.style.display = localEnabled && oidcEnabled ? "flex" : "none";
    els.oidcLoginBtn.style.display = oidcEnabled ? "block" : "none";
    els.oidcLoginBtn.textContent = state.authConfig?.oidc?.label || (state.lang === "en" ? "Login with HDGNSS" : "HDGNSS 登录");
    els.authMessage.textContent = state.authMessage ? decodeURIComponent(state.authMessage) : "";

    if (!state.authenticated) return;

    renderPageTabs();
    renderShellStatus();
    renderSecurityNotice();

    renderOverview();
    renderSources();
    renderMounts();
    renderRelays();
    renderRelayRejects();
    renderAuditEvents();
    renderUsers();
    renderBlocks();
    renderLimits();
    renderMountConfigList();
    renderRelayConfigList();
    renderMountDetail();
    renderAdminProfile();
    renderAuthSettings();
  }

  function renderPageTabs() {
    const mapping = [
      [els.tabHome, els.pageHome, "home"],
      [els.tabUsers, els.pageUsers, "users"],
      [els.tabSettings, els.pageSettings, "settings"],
    ];
    mapping.forEach(([button, page, key]) => {
      const active = state.activePage === key;
      button.classList.toggle("active", active);
      page.classList.toggle("page-hidden", !active);
    });
  }

  function renderShellStatus() {
    const locale = state.lang === "en" ? "en-US" : "zh-CN";
    if (state.lang === "en") {
      els.connectionStatus.textContent = state.loading ? "Loading" : state.connected ? "API Connected" : "Offline Mode";
      els.lastUpdated.textContent = state.lastUpdated
        ? `Updated ${new Intl.DateTimeFormat(locale, { dateStyle: "medium", timeStyle: "medium" }).format(state.lastUpdated)}`
        : "Not yet loaded";
    } else if (state.lang === "zh") {
      els.connectionStatus.textContent = state.loading ? "加载中" : state.connected ? "已连接 API" : "离线模式";
      els.lastUpdated.textContent = state.lastUpdated
        ? `最后更新 ${new Intl.DateTimeFormat(locale, { dateStyle: "medium", timeStyle: "medium" }).format(state.lastUpdated)}`
        : "尚未加载";
    } else {
      els.connectionStatus.textContent = state.loading ? "加载中 Loading" : state.connected ? "已连接 API Connected" : "离线模式 Offline";
      els.lastUpdated.textContent = state.lastUpdated
        ? `最后更新 Updated ${new Intl.DateTimeFormat("zh-CN", { dateStyle: "medium", timeStyle: "medium" }).format(state.lastUpdated)}`
        : "尚未加载 Not yet loaded";
    }
    els.connectionStatus.parentElement.classList.toggle("pulse", state.loading);
    els.connectionStatus.previousElementSibling.style.background = state.connected ? "var(--success)" : state.loading ? "var(--warning)" : "var(--danger)";
  }

  function renderSecurityNotice() {
    const show = !!state.authSession?.user?.requirePasswordChange;
    els.securityNotice.classList.toggle("page-hidden", !show);
  }

  function renderHomeOnly() {
    renderShellStatus();
    renderOverview();
    renderSources();
    renderMounts();
    renderRelays();
    renderRelayRejects();
    renderAuditEvents();
    renderMountDetail();
  }

  function renderOverview() {
    const o = state.overview || fallbackData.overview;
    const lang = state.lang;
    const labels = lang === "en"
      ? [["Online Sources", o.activeSources], ["Online Mountpoints", o.activeMounts], ["Online Clients", o.connectedClients], ["Blocked IPs", o.blockedIPs], ["Active Relays", o.activeRelays], ["Relay Errors", o.relayErrors]]
      : lang === "zh"
        ? [["在线数据源", o.activeSources], ["在线挂载点", o.activeMounts], ["在线客户端", o.connectedClients], ["封锁 IP", o.blockedIPs], ["活跃中继", o.activeRelays], ["中继错误", o.relayErrors]]
        : [["在线数据源 / Online Sources", o.activeSources], ["在线挂载点 / Mountpoints", o.activeMounts], ["在线客户端 / Clients", o.connectedClients], ["封锁 IP / Blocked IPs", o.blockedIPs], ["活跃中继 / Active Relays", o.activeRelays], ["中继错误 / Relay Errors", o.relayErrors]];
    const miniLabels = lang === "en"
      ? [`Throughput ${o.throughputKbps} kbps`, `Decode ${o.decodeCandidates}`, `Relay ${o.activeRelays} / ${o.relayErrors}`]
      : lang === "zh"
        ? [`吞吐 ${o.throughputKbps} kbps`, `解码 ${o.decodeCandidates}`, `中继 ${o.activeRelays} / ${o.relayErrors}`]
        : [`吞吐 ${o.throughputKbps} kbps`, `解码 ${o.decodeCandidates}`, `中继 ${o.activeRelays} / ${o.relayErrors}`];
    els.miniMetrics.innerHTML = miniLabels.map((text) => `<span class="mini-metric">${text}</span>`).join("");
    els.overviewCards.innerHTML = labels.map(([label, value]) => `
      <article class="metric">
        <div class="label">${label}</div>
        <div class="value">${formatValue(value)}</div>
      </article>`).join("");
  }

  function renderSources() {
    const sources = state.connected ? state.sources : fallbackData.sources;
    const emptyMsg = state.lang === "en" ? "No online sources" : state.lang === "zh" ? "当前没有在线数据源" : "当前没有在线数据源 No online sources";
    els.onlineSources.innerHTML = sources.length ? sources.map((source) => `
      <article class="item">
        <div class="item-head"><strong>${formatValue(source.username)}</strong><span class="muted">(${formatValue(source.host)})</span></div>
        <div class="muted">Mount：${formatValue(source.mountpoint)} · Rate：${formatValue(source.bitrateKbps)} kbps</div>
      </article>`).join("") : `<div class="empty">${emptyMsg}</div>`;
  }

  function renderMounts() {
    const mounts = (state.connected ? state.mounts : fallbackData.mounts).filter((mount) => mount.status === "online");
    const mountEmptyMsg = state.lang === "en" ? "No online mountpoints" : state.lang === "zh" ? "当前没有在线挂载点" : "当前没有在线挂载点 No online mountpoints";
    els.mountpoints.innerHTML = mounts.length ? mounts.map((mount) => `
      <article class="item item-clickable ${state.selectedMountId === mount.id ? "item-selected" : ""}" data-mount-card="${mount.id}" tabindex="0" role="button" aria-pressed="${state.selectedMountId === mount.id ? "true" : "false"}">
        <div class="item-head"><strong>${mount.name}</strong><span class="muted">(${mount.region || "Unknown"})</span></div>
        <div class="muted">Source：${formatValue(mount.currentSource)} · Client：${formatValue(mount.clients)}</div>
      </article>`).join("") : `<div class="empty">${mountEmptyMsg}</div>`;

    els.mountpoints.querySelectorAll("[data-mount-card]").forEach((card) => {
      const activate = async () => {
        const mountId = card.getAttribute("data-mount-card");
        setState({ selectedMountId: mountId });
        try {
          const [detail, history] = await Promise.all([api.mountDetail(mountId), api.mountHistory(mountId)]);
          setState({ mountDetailData: detail || null, mountHistory: history || [], message: `已加载挂载点 ${mountId}` });
        } catch {
          setState({ mountDetailData: null, mountHistory: [], message: `使用本地挂载点数据展示 ${mountId}` });
        }
      };
      card.addEventListener("click", activate);
      card.addEventListener("keydown", (event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          activate();
        }
      });
    });
  }

  function relayStateClass(state) {
    switch (state) {
      case "online":
      case "idle":
        return "green";
      case "waiting_gga":
      case "connecting":
      case "reconnecting":
        return "orange";
      case "error":
      case "backoff":
        return "red";
      case "disabled":
      case "mount_disabled":
      case "mount_missing":
      default:
        return "gray";
    }
  }

  function formatRelayUpstream(relay) {
    const host = formatValue(relay.upstreamHost);
    const port = relay.upstreamPort ? relay.upstreamPort : 2101;
    const mount = relay.upstreamMount ? (String(relay.upstreamMount).startsWith("/") ? relay.upstreamMount : `/${relay.upstreamMount}`) : "/";
    return `${host}:${port}${mount}`;
  }

  function formatRelayCluster(relay) {
    const radius = Number(relay.clusterRadiusKm);
    const slots = Number(relay.clusterSlots);
    const radiusText = Number.isFinite(radius) && radius > 0 ? `${radius.toFixed(radius % 1 === 0 ? 0 : 1)} km` : "30 km";
    const slotsText = Number.isFinite(slots) && slots > 0 ? `${slots} slots` : "2 slots";
    return `${radiusText} · ${slotsText}`;
  }

  function formatRelayAccountPool(relay) {
    const poolSize = Number(relay.poolSize ?? relay.accountPool?.length ?? 0);
    const leasedAccounts = Number(relay.leasedAccounts ?? 0);
    const healthy = Number(relay.healthyAccounts ?? 0);
    const unhealthy = Number(relay.unhealthyAccounts ?? 0);
    return lv(
      `池 ${poolSize} · 租用 ${leasedAccounts} · 健康 ${healthy}/${healthy + unhealthy || poolSize}`,
      `Pool ${poolSize} · Leased ${leasedAccounts} · Healthy ${healthy}/${healthy + unhealthy || poolSize}`
    );
  }

  function formatRelayRuntime(relay) {
    const sessions = Number(relay.activeSessions ?? 0);
    const clients = Number(relay.activeClients ?? 0);
    const rejected = Number(relay.rejectedClients ?? 0);
    const retry = Number(relay.retryCount ?? 0);
    return `Sessions ${sessions} · Clients ${clients} · Retry ${retry} · Reject ${rejected}`;
  }

  function formatRelayRejectReason(relay) {
    const reason = typeof relay === "string" ? relay : relay?.lastRejectReason;
    switch (reason) {
      case "pool_exhausted":
        return lv("账号池耗尽", "Pool Exhausted");
      case "source_limit":
        return lv("上游会话已达上限", "Source Limit");
      case "invalid_gga":
        return lv("GGA 无效", "Invalid GGA");
      case "account_backoff":
        return lv("账号退避中", "Account Backoff");
      default:
        return reason ? String(reason) : lv("无", "None");
    }
  }

  function formatRelayFailureReason(relay) {
    const reason = typeof relay === "string" ? relay : relay?.lastFailureReason;
    switch (reason) {
      case "auth_failed_upstream": return lv("上游认证失败", "Auth Failed");
      case "upstream_mount_unavailable": return lv("上游挂载点不可用", "Mount Unavailable");
      case "unexpected_upstream_response": return lv("上游响应异常", "Unexpected Response");
      case "upstream_dns_lookup_failed": return lv("上游 DNS 解析失败", "DNS Lookup Failed");
      case "upstream_connection_refused": return lv("上游拒绝连接", "Connection Refused");
      case "upstream_network_unreachable": return lv("上游网络不可达", "Network Unreachable");
      case "upstream_eof": return lv("上游提前关闭", "Upstream EOF");
      case "upstream_io_error": return lv("上游读写错误", "IO Error");
      case "upstream_timeout": return lv("上游超时", "Timeout");
      case "upstream_closed": return lv("上游关闭连接", "Upstream Closed");
      default: return reason ? String(reason) : lv("无", "None");
    }
  }

  function formatRelayAccountHealth(relay) {
    const items = relay?.accountHealth || [];
    if (!items.length) return lv("未记录", "No records");
    return items.map((item) => {
      const stateText = item.state || (item.healthy ? "healthy" : "error");
      const reasonText = item.lastFailureReason ? ` · ${formatRelayFailureReason(item.lastFailureReason)}` : "";
      return `${item.name || item.username}: ${stateText}${reasonText}`;
    }).join(" / ");
  }

  function renderRelayRejects() {
    if (!els.relayRejects) return;
    const relays = state.connected ? state.relays : fallbackData.relays;
    const items = relays
      .flatMap((relay) => (relay.recentRejects || []).map((event) => ({
        relay: relay.name,
        at: event.at,
        reason: event.reason,
        username: event.username,
        remoteAddr: event.remoteAddr,
        mount: event.mount || relay.localMount,
      })))
      .sort((a, b) => new Date(b.at || 0).getTime() - new Date(a.at || 0).getTime());
    els.relayRejects.innerHTML = items.length
      ? items.slice(0, 10).map((item) => `
        <div class="detail-card"><div class="detail-list">
          <div class="detail-row"><span>${lv("时间", "Time")}</span><strong>${formatTime(item.at)}</strong></div>
          <div class="detail-row"><span>Relay</span><strong>${item.relay}</strong></div>
          <div class="detail-row"><span>${lv("挂载点", "Mount")}</span><strong>${item.mount || lv("未记录", "N/A")}</strong></div>
          <div class="detail-row"><span>${lv("用户", "User")}</span><strong>${item.username || lv("未记录", "N/A")}</strong></div>
          <div class="detail-row"><span>${lv("地址", "Address")}</span><strong class="mono">${item.remoteAddr || lv("未记录", "N/A")}</strong></div>
          <div class="detail-row"><span>${lv("原因", "Reason")}</span><strong>${formatRelayRejectReason(item.reason)}</strong></div>
        </div></div>`).join("")
      : `<div class="empty">${lv("最近没有 Relay 拒绝事件", "No recent relay rejection events")}</div>`;
  }

  function formatAuditAction(item) {
    const action = item?.action || "";
    switch (action) {
      case "auth.login": return lv("管理员登录", "Admin Login");
      case "auth.logout": return lv("管理员登出", "Admin Logout");
      case "settings.admin.update": return lv("管理员设置更新", "Admin Settings Updated");
      case "settings.auth.update": return lv("认证设置更新", "Auth Settings Updated");
      case "user.upsert": return lv("用户保存", "User Saved");
      case "user.delete": return lv("用户删除", "User Deleted");
      case "user.set_enabled": return lv("用户状态更新", "User Status Updated");
      case "mount.upsert": return lv("挂载点保存", "Mountpoint Saved");
      case "mount.delete": return lv("挂载点删除", "Mountpoint Deleted");
      case "mount.set_enabled": return lv("挂载点状态更新", "Mountpoint Status Updated");
      case "relay.upsert": return lv("Relay 保存", "Relay Saved");
      case "relay.delete": return lv("Relay 删除", "Relay Deleted");
      case "relay.set_enabled": return lv("Relay 状态更新", "Relay Status Updated");
      case "relay.session_connect": return lv("Relay 上游连接", "Relay Upstream Connected");
      case "relay.session_disconnect": return lv("Relay 上游断开", "Relay Upstream Disconnected");
      case "relay.session_error": return lv("Relay 上游错误", "Relay Upstream Error");
      case "relay.client_reject": return lv("Relay 拒绝接入", "Relay Client Rejected");
      case "source.connect": return lv("Source 上线", "Source Connected");
      case "source.disconnect": return lv("Source 下线", "Source Disconnected");
      case "block.add": return lv("Block 规则新增", "Block Rule Added");
      case "block.delete": return lv("Block 规则删除", "Block Rule Deleted");
      case "limits.update": return lv("运行配额更新", "Resource Limits Updated");
      default: return action || lv("未命名事件", "Unknown Event");
    }
  }



  function renderAuditEvents() {
    if (!els.auditEvents) return;
    const items = state.connected ? state.auditEvents : fallbackData.auditEvents;
    els.auditEvents.innerHTML = items.length
      ? items.slice(0, 9).map((item) => `
        <div class="detail-card"><div class="detail-list">
          <div class="detail-row"><span>${lv("时间", "Time")}</span><strong>${formatTime(item.at)}</strong></div>
          <div class="detail-row"><span>${lv("事件", "Event")}</span><strong>${formatAuditAction(item)}</strong></div>
          <div class="detail-row"><span>${lv("资源", "Resource")}</span><strong>${formatValue(item.resource)} · ${formatValue(item.resourceId)}</strong></div>
          <div class="detail-row"><span>${lv("操作者", "Actor")}</span><strong>${formatValue(item.actor)}</strong></div>
          <div class="detail-row"><span>${lv("状态", "Status")}</span><strong>${formatValue(item.status)}</strong></div>
          <div class="detail-row"><span>${lv("地址", "Address")}</span><strong class="mono">${formatValue(item.remoteAddr)}</strong></div>
          <div class="detail-row"><span>${lv("说明", "Note")}</span><strong>${formatValue(item.message)}</strong></div>
        </div></div>`).join("")
      : `<div class="empty">${lv("最近没有审计事件", "No recent audit events")}</div>`;
  }

  function parseRelayAccountPool(text) {
    const lines = String(text || "")
      .split(/\r?\n/)
      .map((line) => line.trim())
      .filter(Boolean);
    const accounts = [];
    for (const line of lines) {
      const parts = line.split(",").map((item) => item.trim());
      if (parts.length < 3) continue;
      const [name, username, password] = parts;
      if (!name && !username) continue;
      let enabled = true;
      let expiresAt = "";
      const fourth = parts[3] || "";
      const fifth = parts[4] || "";
      if (fourth) {
        const lower = fourth.toLowerCase();
        if (["true", "1", "yes", "on", "enable", "enabled"].includes(lower)) {
          enabled = true;
        } else if (["false", "0", "no", "off", "disable", "disabled"].includes(lower)) {
          enabled = false;
        } else if (!Number.isNaN(Date.parse(fourth))) {
          expiresAt = new Date(fourth).toISOString();
        }
      }
      if (fifth) {
        if (!Number.isNaN(Date.parse(fifth))) {
          expiresAt = new Date(fifth).toISOString();
        } else {
          const lower = fifth.toLowerCase();
          if (["true", "1", "yes", "on", "enable", "enabled"].includes(lower)) {
            enabled = true;
          } else if (["false", "0", "no", "off", "disable", "disabled"].includes(lower)) {
            enabled = false;
          }
        }
      }
      accounts.push({
        name: name || username,
        username,
        password,
        enabled,
        expiresAt,
      });
    }
    return accounts;
  }

  function formatRelayAccountPoolText(accounts) {
    return (accounts || []).map((account) => {
      const fields = [
        account.name || "",
        account.username || "",
        account.password || "",
        account.enabled === false ? "false" : "true",
      ];
      if (account.expiresAt) {
        fields.push(account.expiresAt);
      }
      return fields.join(",");
    }).join("\n");
  }

  function renderRelays() {
    const relays = (state.connected ? state.relays : fallbackData.relays).filter((relay) => {
      const activeSessions = Number(relay.activeSessions ?? 0);
      const activeClients = Number(relay.activeClients ?? 0);
      return activeSessions > 0 || activeClients > 0;
    });
    if (!els.relayTable) return;
    els.relayTable.innerHTML = relays.length ? relays.map((relay) => `
      <tr>
        <td><span class="tag ${relayStateClass(relay.state)}">${relay.state || "unknown"}</span></td>
        <td>
          <strong>${relay.name}</strong>
          <div class="muted">Local：${formatValue(relay.localMount)}</div>
        </td>
        <td class="mono">
          <div>${formatRelayUpstream(relay)}</div>
          <div class="muted">NTRIP ${relay.ntripVersion || 1}</div>
        </td>
        <td>
          <div>${formatRelayAccountPool(relay)}</div>
          <div class="muted">Pool ${formatValue(relay.poolSize)} · Radius ${formatValue(relay.clusterRadiusKm)} km</div>
        </td>
        <td>
          <div>${formatRelayRuntime(relay)}</div>
          <div class="muted">Leased ${formatValue(relay.leasedAccounts)} · ${formatRelayRejectReason(relay)} · ${formatRelayFailureReason(relay)}</div>
        </td>
        <td>
          <div>${formatRelayCluster(relay)}</div>
          <div class="muted">${relay.staticGgaEnabled ? lv("静态 GGA", "Static GGA") : lv("动态 GGA", "Dynamic GGA")} · ${relay.nextRetryAt ? `${lv("下次重试", "Next retry")} ${formatTime(relay.nextRetryAt)}` : lv("未退避", "No backoff")}</div>
        </td>
      </tr>`).join("") : `<tr><td colspan="6" class="empty">${lv("当前没有活跃中继", "No active relays")}</td></tr>`;
  }

  function renderUsers() {
    const users = state.userPageResult?.items || [];
    const total = state.userPageResult?.total || 0;
    const page = state.userPageResult?.page || 1;
    const pageSize = state.userPageResult?.page_size || 25;
    if (els.userCount) {
      els.userCount.textContent = `${users.length} / ${total}`;
    }
    if (els.userPageInfo) {
      const pages = Math.max(1, Math.ceil(total / pageSize));
      els.userPageInfo.textContent = lv(`第 ${page} / ${pages} 页`, `Page ${page} / ${pages}`);
    }
    if (els.userPrevBtn) {
      els.userPrevBtn.disabled = page <= 1;
    }
    if (els.userNextBtn) {
      els.userNextBtn.disabled = page * pageSize >= total;
    }
    els.userTable.innerHTML = users.length ? users.map((user) => `
      <tr>
        <td>${user.type}</td>
        <td>${user.username}</td>
        <td>${(user.permissions || []).join(", ")}</td>
        <td><span class="tag ${user.status === "active" ? "green" : "gray"}">${user.status || "active"}</span></td>
        <td class="action-row">
          <button class="button button-secondary" type="button" data-user-edit="${user.type}:${user.username}">${lv("编辑", "Edit")}</button>
          <button class="button button-secondary" type="button" data-user-toggle="${user.type}:${user.username}" data-user-enabled="${user.status === "active" ? "true" : "false"}">${user.status === "active" ? lv("禁用", "Disable") : lv("启用", "Enable")}</button>
          <button class="button button-secondary button-danger" type="button" data-user-delete="${user.type}:${user.username}">${lv("删除", "Delete")}</button>
        </td>
      </tr>`).join("") : `<tr><td colspan="5" class="empty">${lv("没有匹配的用户", "No matching users")}</td></tr>`;

    els.userTable.querySelectorAll("[data-user-edit]").forEach((button) => {
      button.addEventListener("click", () => {
        const [type, username] = button.getAttribute("data-user-edit").split(":");
        const user = users.find((item) => item.type === type && item.username === username);
        if (!user) return;
        els.userForm.mode.value = "edit";
        els.userForm.role.value = user.type;
        els.userForm.username.value = user.username;
        els.userForm.password.value = "";
        els.userForm.mounts.value = (user.permissions || []).join(",");
        els.userForm.note.value = user.note || "";
        setState({ message: `已载入用户 ${username}，留空密码表示不修改密码` });
      });
    });
    els.userTable.querySelectorAll("[data-user-toggle]").forEach((button) => {
      button.addEventListener("click", async () => {
        const [type, username] = button.getAttribute("data-user-toggle").split(":");
        const enabled = button.getAttribute("data-user-enabled") !== "true";
        try {
          await api.setUserEnabled(type, username, enabled);
          await reload();
        } catch {
          setState({ message: `切换用户 ${username} 状态失败` });
        }
      });
    });
    els.userTable.querySelectorAll("[data-user-delete]").forEach((button) => {
      button.addEventListener("click", async () => {
        const [type, username] = button.getAttribute("data-user-delete").split(":");
        if (!window.confirm(`确认删除用户 ${username}？`)) return;
        try {
          await api.deleteUser(type, username);
          await reload();
        } catch {
          setState({ message: `删除用户 ${username} 失败` });
        }
      });
    });
  }

  function renderBlocks() {
    const blocks = state.connected ? state.blocks : fallbackData.blocks;
    els.blockTable.innerHTML = blocks.length ? blocks.map((block) => `
      <tr>
        <td>${block.ip}</td><td>${block.reason}</td><td>${formatTime(block.expiresAt)}</td>
        <td><button class="button button-secondary" type="button" data-delete-block="${block.ip}">删除</button></td>
      </tr>`).join("") : `<tr><td colspan="4" class="empty">${state.lang === "en" ? "No block rules" : state.lang === "zh" ? "当前没有封锁规则" : "当前没有封锁规则 No block rules"}</td></tr>`;
    els.blockTable.querySelectorAll("[data-delete-block]").forEach((button) => {
      button.addEventListener("click", async () => {
        const ip = button.getAttribute("data-delete-block");
        try {
          await api.deleteBlock(ip);
          await reload();
        } catch {
          setState({ message: `删除 Block ${ip} 失败` });
        }
      });
    });
  }

  function renderLimits() {
    const limits = state.limits || fallbackData.limits;
    const quotaLabels = state.lang === "en"
      ? [["Max Clients", limits.maxClients], ["Max Sources", limits.maxSources], ["Max Pending", limits.maxPending], ["Max Connections", limits.maxConnections]]
      : state.lang === "zh"
        ? [["最大 Client", limits.maxClients], ["最大 Source", limits.maxSources], ["最大 Pending", limits.maxPending], ["最大并发连接", limits.maxConnections]]
        : [["最大 Client / Max Clients", limits.maxClients], ["最大 Source / Max Sources", limits.maxSources], ["最大 Pending / Max Pending", limits.maxPending], ["最大并发连接 / Max Connections", limits.maxConnections]];
    els.quotaGrid.innerHTML = quotaLabels
      .map(([label, value]) => `<div class="quota"><div class="k">${label}</div><div class="v">${formatValue(value)}</div></div>`).join("");
    els.limitsForm.maxClients.value = limits.maxClients ?? 0;
    els.limitsForm.maxSources.value = limits.maxSources ?? 0;
    els.limitsForm.maxPending.value = limits.maxPending ?? 0;
    els.limitsForm.maxConnections.value = limits.maxConnections ?? 0;
  }

  function renderAdminProfile() {
    if (!els.adminProfileForm) return;
    const admin = state.adminProfile;
    if (!admin) return;
    els.adminProfileForm.enabled.value = admin.enabled === false ? "false" : "true";
    els.adminProfileForm.username.value = admin.username || "";
    els.adminProfileForm.password.value = "";
  }

  function renderAuthSettings() {
    if (!els.authSettingsForm) return;
    const authSettings = state.authSettings;
    if (!authSettings) return;
    els.authSettingsForm.oidcEnabled.value = authSettings.oidc?.enabled ? "true" : "false";
    els.authSettingsForm.provider.value = authSettings.oidc?.provider || "pocketid";
    els.authSettingsForm.issuerURL.value = authSettings.oidc?.issuerURL || "";
    els.authSettingsForm.clientID.value = authSettings.oidc?.clientID || "";
    els.authSettingsForm.clientSecret.value = authSettings.oidc?.clientSecret || "";
    els.authSettingsForm.redirectURL.value = authSettings.oidc?.redirectURL || "";
  }

  function renderMountDetail() {
    const mounts = state.connected ? state.mounts : fallbackData.mounts;
    const activeId = state.selectedMountId;
    const selected = activeId ? mounts.find((m) => m.id === activeId && m.status === "online") : null;
    const detail = state.mountDetailData && state.mountDetailData.id === activeId ? state.mountDetailData : null;
    const history = state.mountHistory || [];
    const source = (state.connected ? state.sources : fallbackData.sources).find((item) => item.id === selected?.sourceId);
    const relay = detail?.relay || (state.connected ? state.relays.find((item) => item.localMount === selected?.id) : null);
    if (!selected) {
      els.mountDetail.innerHTML = `<div class="empty">${lv("未选择在线挂载点", "No mountpoint selected")}</div>`;
      return;
    }
    const na = lv("未配置", "N/A");
    const detailRows = [];
    if (detail?.sourceUsername) {
      detailRows.push(`<div class="detail-row"><span>${lv("默认 Source", "Default Source")}</span><strong>${detail.sourceUsername}</strong></div>`);
    }
    if ((detail?.allowedSources || []).length) {
      detailRows.push(`<div class="detail-row"><span>${lv("可接入 Source", "Allowed Sources")}</span><strong>${detail.allowedSources.join(", ")}</strong></div>`);
    }
    if ((detail?.allowedClients || []).length) {
      detailRows.push(`<div class="detail-row"><span>${lv("可接入 Client", "Allowed Clients")}</span><strong>${detail.allowedClients.join(", ")}</strong></div>`);
    }
    els.mountDetail.innerHTML = `
      <div class="detail-card"><h3>${selected.name}</h3><div class="detail-list">
        <div class="detail-row"><span>${lv("挂载点 ID", "Mount ID")}</span><strong>${selected.id}</strong></div>
        <div class="detail-row"><span>${lv("状态", "Status")}</span><strong>${selected.status}</strong></div>
        <div class="detail-row"><span>${lv("配置启用", "Enabled")}</span><strong>${selected.enabled !== false ? lv("是", "Yes") : lv("否", "No")}</strong></div>
        <div class="detail-row"><span>${lv("区域", "Region")}</span><strong>${selected.region}</strong></div>
        <div class="detail-row"><span>${lv("在线 Client", "Online Clients")}</span><strong>${selected.clients}</strong></div>
        ${detailRows.join("")}
      </div></div>
      <div class="detail-card"><h3>${lv("元数据", "Metadata")}</h3><div class="detail-list">
        <div class="detail-row"><span>${lv("描述", "Description")}</span><strong>${formatValue(detail?.description || selected.description)}</strong></div>
        <div class="detail-row"><span>${lv("关联 Source", "Source")}</span><strong>${source?.username || lv("未绑定", "None")}</strong></div>
        <div class="detail-row"><span>${lv("关联 Relay", "Relay")}</span><strong>${relay?.name || na}</strong></div>
        <div class="detail-row"><span>${lv("支持星系", "Constellations")}</span><strong>${(detail?.constellations || source?.galaxies || [lv("待 API 填充", "Pending")]).join(", ")}</strong></div>
        <div class="detail-row"><span>${lv("宣告 RTCM", "Advertised RTCM")}</span><strong>${(detail?.advertisedRtcm || []).join(", ") || lv("未设置", "N/A")}</strong></div>
        <div class="detail-row"><span>${lv("配置位置", "Position")}</span><strong>${formatGeoPoint(detail?.position)}</strong></div>
      </div></div>
      <div class="detail-card"><h3>${lv("Relay 运行态", "Relay Runtime")}</h3><div class="detail-list">
        <div class="detail-row"><span>${lv("Relay 状态", "Relay State")}</span><strong>${relay?.state || na}</strong></div>
        <div class="detail-row"><span>${lv("上游", "Upstream")}</span><strong class="mono">${relay ? formatRelayUpstream(relay) : na}</strong></div>
        <div class="detail-row"><span>${lv("账号池", "Account Pool")}</span><strong>${relay ? formatRelayAccountPool(relay) : na}</strong></div>
        <div class="detail-row"><span>${lv("账号健康", "Account Health")}</span><strong>${relay ? formatRelayAccountHealth(relay) : na}</strong></div>
        <div class="detail-row"><span>${lv("运行态", "Runtime")}</span><strong>${relay ? formatRelayRuntime(relay) : na}</strong></div>
        <div class="detail-row"><span>${lv("聚类", "Cluster")}</span><strong>${relay ? formatRelayCluster(relay) : na}</strong></div>
        <div class="detail-row"><span>${lv("拒绝原因", "Reject Reason")}</span><strong>${relay ? formatRelayRejectReason(relay) : lv("无", "None")}</strong></div>
        <div class="detail-row"><span>${lv("拒绝时间", "Last Rejected")}</span><strong>${relay?.lastRejectAt ? formatTime(relay.lastRejectAt) : lv("未记录", "N/A")}</strong></div>
        <div class="detail-row"><span>${lv("失败原因", "Failure Reason")}</span><strong>${relay ? formatRelayFailureReason(relay) : lv("无", "None")}</strong></div>
        <div class="detail-row"><span>${lv("重试次数", "Retry Count")}</span><strong>${formatValue(relay?.retryCount)}</strong></div>
        <div class="detail-row"><span>${lv("下次重试", "Next Retry")}</span><strong>${relay?.nextRetryAt ? formatTime(relay.nextRetryAt) : lv("未计划", "N/A")}</strong></div>
        <div class="detail-row"><span>${lv("上次成功", "Last Success")}</span><strong>${relay?.lastSuccessfulAt ? formatTime(relay.lastSuccessfulAt) : lv("未记录", "N/A")}</strong></div>
        <div class="detail-row"><span>${lv("上次 GGA", "Last GGA")}</span><strong>${relay?.lastGgaAt ? formatTime(relay.lastGgaAt) : lv("未记录", "N/A")}</strong></div>
        <div class="detail-row"><span>${lv("最近错误", "Last Error")}</span><strong>${relay?.lastError || lv("无", "None")}</strong></div>
      </div></div>
      <div class="detail-card"><h3>${lv("RTCM 运行态", "RTCM Runtime")}</h3><div class="detail-list">
        <div class="detail-row"><span>${lv("RTCM 解码位置", "Decoded Position")}</span><strong>${detail?.runtime?.decodedPosition ? formatGeoPoint(detail.runtime.decodedPosition) : lv("未解出", "N/A")}</strong></div>
        <div class="detail-row"><span>${lv("编码", "Encoding")}</span><strong>${source?.encoding || "RTCM 3"}</strong></div>
        <div class="detail-row"><span>${lv("观测消息", "Messages")}</span><strong>${(detail?.runtime?.messageTypes || source?.messages || []).join(", ") || lv("暂无", "None")}</strong></div>
        <div class="detail-row"><span>${lv("候选星系", "Constellations")}</span><strong>${(detail?.runtime?.constellations || source?.galaxies || []).join(", ") || lv("暂无", "None")}</strong></div>
        <div class="detail-row"><span>${lv("MSM 分类", "MSM Classes")}</span><strong>${formatMSMClasses(detail?.runtime)}</strong></div>
        <div class="detail-row"><span>${lv("MSM 星系", "MSM Families")}</span><strong>${formatMSMFamilies(detail?.runtime)}</strong></div>
        <div class="detail-row"><span>${lv("观测帧数", "Frames Observed")}</span><strong>${formatValue(detail?.runtime?.framesObserved)}</strong></div>
        ${formatReferenceStation(detail?.runtime?.reference).map(([label, value]) => `<div class="detail-row"><span>${label}</span><strong>${formatValue(value)}</strong></div>`).join("")}
      </div></div>
      <div class="detail-card"><h3>${lv("当前状态", "Current State")}</h3><div class="detail-list">
        <div class="detail-row"><span>${lv("消息", "Message")}</span><strong>${state.message}</strong></div>
        <div class="detail-row"><span>${lv("数据源", "Data Source")}</span><strong>${state.connected ? "API" : lv("本地回退数据", "Local Fallback")}</strong></div>
        <div class="detail-row"><span>${lv("更新时间", "Updated")}</span><strong>${state.lastUpdated ? formatTime(state.lastUpdated) : lv("未更新", "N/A")}</strong></div>
        <div class="detail-row"><span>${lv("最近活跃", "Last Active")}</span><strong>${formatTime(detail?.runtime?.lastActive)}</strong></div>
        <div class="detail-row"><span>${lv("字节入/出", "Bytes In/Out")}</span><strong class="mono">${formatValue(detail?.runtime?.bytesIn)} / ${formatValue(detail?.runtime?.bytesOut)}</strong></div>
      </div></div>
      <div class="detail-card"><h3>${lv("最近历史样本", "Recent Samples")}</h3><div class="detail-list">
        ${history.length ? history.slice(0, 8).map((item) => `<div class="detail-row"><span>${formatTime(item.sample_time || item.sampleTime)}</span><strong class="mono">in ${formatValue(item.bytes_in || item.bytesIn)} / out ${formatValue(item.bytes_out || item.bytesOut)} / cli ${formatValue(item.client_count || item.clientCount)}</strong></div>`).join("") : `<div class="empty">${lv("暂无历史样本，SQLite 后端运行一段时间后会出现。", "No history samples yet. They will appear after the SQLite backend has been running for a while.")}</div>`}
      </div></div>`;
  }

  function renderMountConfigList() {
    const mounts = state.mountAdminPageResult?.items || [];
    const total = state.mountAdminPageResult?.total || 0;
    const page = state.mountAdminPageResult?.page || 1;
    const pageSize = state.mountAdminPageResult?.page_size || 25;
    if (!els.mountConfigList) return;
    if (els.mountCount) {
      els.mountCount.textContent = `${mounts.length} / ${total}`;
    }
    if (els.mountPageInfo) {
      const pages = Math.max(1, Math.ceil(total / pageSize));
      els.mountPageInfo.textContent = lv(`第 ${page} / ${pages} 页`, `Page ${page} / ${pages}`);
    }
    if (els.mountPrevBtn) {
      els.mountPrevBtn.disabled = page <= 1;
    }
    if (els.mountNextBtn) {
      els.mountNextBtn.disabled = page * pageSize >= total;
    }
    els.mountConfigList.innerHTML = mounts.length ? mounts.map((mount) => `
      <tr>
        <td>${mount.name}</td>
        <td class="action-row">
          <button class="button button-secondary" type="button" data-config-edit="${mount.id}">${lv("编辑", "Edit")}</button>
          <button class="button button-secondary" type="button" data-config-toggle="${mount.id}" data-config-enabled="${mount.enabled !== false ? "true" : "false"}">${mount.enabled !== false ? lv("停用", "Disable") : lv("启用", "Enable")}</button>
          <button class="button button-secondary button-danger" type="button" data-config-delete="${mount.id}">${lv("删除", "Delete")}</button>
        </td>
      </tr>`).join("") : `<tr><td colspan="2" class="empty">${lv("没有匹配的挂载点", "No matching mountpoints")}</td></tr>`;

    els.mountConfigList.querySelectorAll("[data-config-edit]").forEach((button) => {
      button.addEventListener("click", async () => {
        const mountId = button.getAttribute("data-config-edit");
        try {
          const detail = await api.mountDetail(mountId);
          fillMountForm(detail || { id: mountId, name: mountId });
          setState({ message: `已载入挂载点 ${mountId} 配置` });
        } catch {
          setState({ message: `载入挂载点 ${mountId} 配置失败` });
        }
      });
    });

    els.mountConfigList.querySelectorAll("[data-config-toggle]").forEach((button) => {
      button.addEventListener("click", async () => {
        const mountId = button.getAttribute("data-config-toggle");
        const enabled = button.getAttribute("data-config-enabled") !== "true";
        try {
          await api.setMountEnabled(mountId, enabled);
          await reload();
        } catch {
          setState({ message: `切换挂载点 ${mountId} 状态失败` });
        }
      });
    });

    els.mountConfigList.querySelectorAll("[data-config-delete]").forEach((button) => {
      button.addEventListener("click", async () => {
        const mountId = button.getAttribute("data-config-delete");
        if (!window.confirm(`确认删除挂载点 ${mountId}？`)) return;
        try {
          await api.deleteMount(mountId);
          if (state.selectedMountId === mountId) {
            setState({ selectedMountId: null, mountDetailData: null, mountHistory: [] });
          }
          await reload();
        } catch {
          setState({ message: `删除挂载点 ${mountId} 失败` });
        }
      });
    });
  }

  function renderRelayConfigList() {
    const relays = state.relayPageResult?.items || [];
    const total = state.relayPageResult?.total || 0;
    const page = state.relayPageResult?.page || 1;
    const pageSize = state.relayPageResult?.page_size || 25;
    if (!els.relayConfigList) return;
    if (els.relayCount) {
      els.relayCount.textContent = `${relays.length} / ${total}`;
    }
    if (els.relayPageInfo) {
      const pages = Math.max(1, Math.ceil(total / pageSize));
      els.relayPageInfo.textContent = lv(`第 ${page} / ${pages} 页`, `Page ${page} / ${pages}`);
    }
    if (els.relayPrevBtn) {
      els.relayPrevBtn.disabled = page <= 1;
    }
    if (els.relayNextBtn) {
      els.relayNextBtn.disabled = page * pageSize >= total;
    }
    els.relayConfigList.innerHTML = relays.length ? relays.map((relay) => `
      <tr>
        <td>${relay.name}</td>
        <td class="action-row">
          <button class="button button-secondary" type="button" data-relay-edit="${relay.name}">${lv("编辑", "Edit")}</button>
          <button class="button button-secondary" type="button" data-relay-toggle="${relay.name}" data-relay-enabled="${relay.enabled ? "true" : "false"}">${relay.enabled ? lv("停用", "Disable") : lv("启用", "Enable")}</button>
          <button class="button button-secondary button-danger" type="button" data-relay-delete="${relay.name}">${lv("删除", "Delete")}</button>
        </td>
      </tr>`).join("") : `<tr><td colspan="2" class="empty">${lv("没有匹配的中继", "No matching relays")}</td></tr>`;

    els.relayConfigList.querySelectorAll("[data-relay-edit]").forEach((button) => {
      button.addEventListener("click", () => {
        const name = button.getAttribute("data-relay-edit");
        const relay = relays.find((item) => item.name === name);
        if (!relay) return;
        fillRelayForm(relay);
        setState({ message: `已载入中继 ${name} 配置，请重新填写密码后保存` });
      });
    });
    els.relayConfigList.querySelectorAll("[data-relay-toggle]").forEach((button) => {
      button.addEventListener("click", async () => {
        const name = button.getAttribute("data-relay-toggle");
        const enabled = button.getAttribute("data-relay-enabled") !== "true";
        try {
          await api.setRelayEnabled(name, enabled);
          await reload();
        } catch {
          setState({ message: `切换中继 ${name} 状态失败` });
        }
      });
    });
    els.relayConfigList.querySelectorAll("[data-relay-delete]").forEach((button) => {
      button.addEventListener("click", async () => {
        const name = button.getAttribute("data-relay-delete");
        if (!window.confirm(`确认删除中继 ${name}？`)) return;
        try {
          await api.deleteRelay(name);
          await reload();
        } catch {
          setState({ message: `删除中继 ${name} 失败` });
        }
      });
    });
  }

  async function loadUserPage(page = 1) {
    if (!state.authenticated) return;
    try {
      const result = await api.usersPage(page, state.userPageResult?.page_size || 25, String(els.userSearch?.value || ""));
      setState({ userPageResult: result || { items: [], total: 0, page, page_size: 25 } });
    } catch {
      setState({ message: "加载用户列表失败" });
    }
  }

  async function loadMountAdminPage(page = 1) {
    if (!state.authenticated) return;
    try {
      const result = await api.mountsPage(page, state.mountAdminPageResult?.page_size || 25, String(els.mountSearch?.value || ""));
      setState({ mountAdminPageResult: result || { items: [], total: 0, page, page_size: 25 } });
    } catch {
      setState({ message: "加载挂载点列表失败" });
    }
  }

  async function loadRelayPage(page = 1) {
    if (!state.authenticated) return;
    try {
      const result = await api.relaysPage(page, state.relayPageResult?.page_size || 25, String(els.relaySearch?.value || ""));
      setState({ relayPageResult: result || { items: [], total: 0, page, page_size: 25 } });
    } catch {
      setState({ message: "加载中继列表失败" });
    }
  }

  function fillMountForm(detail) {
    if (!detail || !els.mountForm) return;
    els.mountForm.name.value = detail.name || detail.id || "";
    els.mountForm.description.value = detail.description || "";
    els.mountForm.constellations.value = (detail.constellations || []).join(",");
    els.mountForm.rtcmMessages.value = (detail.advertisedRtcm || []).join(",");
    els.mountForm.decodeCandidate.value = detail.decodeCandidate === false ? "false" : "true";
    els.mountForm.latitude.value = detail.position?.latitude ?? "";
    els.mountForm.longitude.value = detail.position?.longitude ?? "";
  }

  function fillRelayForm(detail) {
    if (!detail || !els.relayForm) return;
    els.relayForm.name.value = detail.name || "";
    els.relayForm.description.value = detail.description || "";
    els.relayForm.enabled.value = detail.enabled === false ? "false" : "true";
    els.relayForm.localMount.value = detail.localMount || "";
    els.relayForm.upstreamHost.value = detail.upstreamHost || "";
    els.relayForm.upstreamPort.value = detail.upstreamPort ?? 2101;
    els.relayForm.upstreamMount.value = detail.upstreamMount || "";
    els.relayForm.username.value = detail.username || "";
    els.relayForm.password.value = "";
    els.relayForm.clusterRadiusKm.value = detail.clusterRadiusKm ?? 30;
    els.relayForm.clusterSlots.value = detail.clusterSlots ?? 2;
    els.relayForm.ntripVersion.value = detail.ntripVersion === 2 ? "2" : "1";
    els.relayForm.accountPool.value = formatRelayAccountPoolText(detail.accountPool || []);
    els.relayForm.ggaSentence.value = detail.ggaSentence || "";
    els.relayForm.ggaIntervalSeconds.value = detail.ggaIntervalSeconds ?? "";
  }

  async function initializeAuth() {
    try {
      const [authConfig, authSession] = await Promise.all([api.authConfig(), api.authSession()]);
      setState({
        authConfig,
        authSession,
        authenticated: !!authSession?.authenticated,
        authMessage: state.authMessage,
      });
      if (authSession?.authenticated) {
        await reload();
        await Promise.all([loadUserPage(1), loadMountAdminPage(1), loadRelayPage(1)]);
        syncHomeAutoRefresh();
      } else {
        setState({ loading: false });
        syncHomeAutoRefresh();
      }
    } catch {
      setState({
        authConfig: { local: { enabled: true, label: "用户名密码登录" }, oidc: { enabled: false } },
        authenticated: false,
        authMessage: "认证配置读取失败，请稍后刷新重试",
        loading: false,
      });
      syncHomeAutoRefresh();
    }
  }

  async function refreshSelectedMountDetail(mountId) {
    if (!mountId) {
      setState({ mountDetailData: null, mountHistory: [] });
      return;
    }
    try {
      const [detail, history] = await Promise.all([api.mountDetail(mountId), api.mountHistory(mountId)]);
      setState({ mountDetailData: detail || null, mountHistory: history || [] });
    } catch {
      setState({ mountDetailData: null, mountHistory: [] });
    }
  }

  async function reload() {
    setState({ loading: true });
    try {
      const [overview, sources, mounts, relays, auditEvents, users, blocks, limits, authConfig, authSession, adminProfile, authSettings] = await Promise.all([
        api.overview(),
        api.sources(),
        api.mounts(),
        api.relays(),
        api.audit(20),
        api.users(),
        api.blocks(),
        api.limits(),
        api.authConfig(),
        api.authSession(),
        api.adminProfile(),
        api.authSettings(),
      ]);
      const nextSelectedMountId = mounts?.some((mount) => mount.id === state.selectedMountId && mount.status === "online")
        ? state.selectedMountId
        : null;
      setState({
        connected: true,
        loading: false,
        authenticated: !!authSession?.authenticated,
        lastUpdated: new Date(),
        overview: overview || fallbackData.overview,
        sources: sources || fallbackData.sources,
        mounts: mounts || fallbackData.mounts,
        relays: relays || fallbackData.relays,
        auditEvents: auditEvents || fallbackData.auditEvents,
        users: users || fallbackData.users,
        blocks: blocks || fallbackData.blocks,
        limits: limits || fallbackData.limits,
        authConfig,
        authSession,
        adminProfile,
        authSettings,
        selectedMountId: nextSelectedMountId,
        mountDetailData: null,
        mountHistory: [],
        message: "API 数据已同步",
      });
      await refreshSelectedMountDetail(nextSelectedMountId);
      await Promise.all([loadUserPage(state.userPageResult?.page || 1), loadMountAdminPage(state.mountAdminPageResult?.page || 1), loadRelayPage(state.relayPageResult?.page || 1)]);
      syncHomeAutoRefresh();
    } catch (error) {
      if (error.code === 401) {
        setState({
          authenticated: false,
          loading: false,
          connected: false,
          authMessage: "登录已失效，请重新登录",
          adminProfile: null,
          authSettings: null,
          overview: fallbackData.overview,
          sources: [],
          mounts: [],
          relays: [],
          users: [],
          blocks: [],
          limits: fallbackData.limits,
          mountDetailData: null,
          mountHistory: [],
        });
        syncHomeAutoRefresh();
        return;
      }
      setState({
        connected: false,
        loading: false,
        lastUpdated: new Date(),
        overview: fallbackData.overview,
        sources: fallbackData.sources,
        mounts: fallbackData.mounts,
        relays: fallbackData.relays,
        users: fallbackData.users,
        blocks: fallbackData.blocks,
        limits: fallbackData.limits,
        selectedMountId: null,
        mountDetailData: null,
        mountHistory: [],
        message: "API 未就绪，已使用本地回退数据",
      });
      syncHomeAutoRefresh();
    }
  }

  async function refreshHomeData() {
    if (!state.authenticated) return;
    Object.assign(state, { loading: true });
    renderShellStatus();
    try {
      const [overview, sources, mounts, relays, auditEvents] = await Promise.all([api.overview(), api.sources(), api.mounts(), api.relays(), api.audit(9)]);
      const nextSelectedMountId = mounts?.some((mount) => mount.id === state.selectedMountId && mount.status === "online")
        ? state.selectedMountId
        : null;
      Object.assign(state, {
        connected: true,
        loading: false,
        lastUpdated: new Date(),
        overview: overview || fallbackData.overview,
        sources: sources || fallbackData.sources,
        mounts: mounts || fallbackData.mounts,
        relays: relays || fallbackData.relays,
        auditEvents: auditEvents || fallbackData.auditEvents,
        selectedMountId: nextSelectedMountId,
        mountDetailData: null,
        mountHistory: [],
        message: state.lang === "en" ? "Status data auto-synced" : state.lang === "zh" ? "状态数据已自动同步" : "状态数据已自动同步 Auto-synced",
      });
      await refreshSelectedMountDetail(nextSelectedMountId);
      renderHomeOnly();
    } catch (error) {
      if (error.code === 401) {
        setState({
          authenticated: false,
          loading: false,
          connected: false,
          authMessage: "登录已失效，请重新登录",
          adminProfile: null,
          authSettings: null,
          overview: fallbackData.overview,
          sources: [],
          mounts: [],
          relays: [],
          mountDetailData: null,
          mountHistory: [],
        });
        syncHomeAutoRefresh();
        return;
      }
      Object.assign(state, {
        connected: false,
        loading: false,
        lastUpdated: new Date(),
        overview: fallbackData.overview,
        sources: fallbackData.sources,
        mounts: fallbackData.mounts,
        relays: fallbackData.relays,
        selectedMountId: null,
        mountDetailData: null,
        mountHistory: [],
        message: state.lang === "en" ? "Auto-refresh failed, using local fallback data" : state.lang === "zh" ? "自动刷新失败，已使用本地回退数据" : "自动刷新失败 Auto-refresh failed，已使用本地回退数据",
      });
      renderHomeOnly();
    }
  }

  function setActivePage(page) {
    setState({ activePage: page });
    if (page === "users") {
      loadUserPage(state.userPageResult?.page || 1);
      loadMountAdminPage(state.mountAdminPageResult?.page || 1);
      loadRelayPage(state.relayPageResult?.page || 1);
    }
    syncHomeAutoRefresh();
  }

  function syncHomeAutoRefresh() {
    if (homeRefreshTimer) {
      clearInterval(homeRefreshTimer);
      homeRefreshTimer = null;
    }
    if (state.authenticated && state.activePage === "home") {
      homeRefreshTimer = setInterval(() => {
        refreshHomeData();
      }, 10000);
    }
  }

  els.loginForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(els.loginForm);
    try {
      await api.login({ username: form.get("username"), password: form.get("password") });
      history.replaceState({}, "", window.location.pathname);
      setState({ authenticated: true, authMessage: "" });
      await reload();
      syncHomeAutoRefresh();
    } catch {
      setState({ authMessage: "用户名或密码错误" });
    }
  });

  els.oidcLoginBtn.addEventListener("click", () => api.startOIDC());
  els.tabHome.addEventListener("click", () => setActivePage("home"));
  els.tabUsers.addEventListener("click", () => setActivePage("users"));
  els.tabSettings.addEventListener("click", () => setActivePage("settings"));
  els.openSettingsFromNotice?.addEventListener("click", () => setActivePage("settings"));

  // Language switcher
  document.getElementById("langSelect")?.addEventListener("change", (e) => {
    const lang = e.target.value;
    localStorage.setItem("hdcaster_lang", lang);
    setState({ lang });
  });
  els.logoutBtn.addEventListener("click", async () => {
    await api.logout();
    setState({ authenticated: false, authSession: null, adminProfile: null, authSettings: null, authMessage: "" });
    syncHomeAutoRefresh();
  });
  els.refreshBtn.addEventListener("click", reload);
  els.backupSqliteBtn.addEventListener("click", () => { api.download("/system/backup.sqlite3"); setState({ message: "已请求备份 SQLite" }); });

  els.userForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(els.userForm);
    const mode = String(form.get("mode") || "create");
    const password = String(form.get("password") || "");
    if (mode !== "edit" && password === "") {
      setState({ message: "新增用户时必须填写密码" });
      return;
    }
    const payload = {
      type: form.get("role"),
      username: form.get("username"),
      password,
      permissions: String(form.get("mounts") || "").split(",").map((item) => item.trim()).filter(Boolean),
      note: form.get("note"),
    };
    await api.saveUser(payload);
    els.userForm.reset();
    els.userForm.mode.value = "create";
    await reload();
  });

  els.relayForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(els.relayForm);
    const accountPool = parseRelayAccountPool(String(form.get("accountPool") || ""));
    const payload = {
      name: String(form.get("name") || "").trim(),
      description: String(form.get("description") || "").trim(),
      enabled: String(form.get("enabled") || "true") !== "false",
      localMount: String(form.get("localMount") || "").trim(),
      upstreamHost: String(form.get("upstreamHost") || "").trim(),
      upstreamPort: Number(form.get("upstreamPort") || 2101),
      upstreamMount: String(form.get("upstreamMount") || "").trim(),
      username: String(form.get("username") || "").trim(),
      password: String(form.get("password") || ""),
      accountPool,
      ntripVersion: Number(form.get("ntripVersion") || 1),
      clusterRadiusKm: Number(form.get("clusterRadiusKm") || 30),
      clusterSlots: Number(form.get("clusterSlots") || 2),
      ggaSentence: String(form.get("ggaSentence") || "").trim(),
      ggaIntervalSeconds: Number(form.get("ggaIntervalSeconds") || 0),
    };
    await api.saveRelay(payload);
    els.relayForm.reset();
    await reload();
  });

  els.userSearch?.addEventListener("input", () => loadUserPage(1));
  els.mountSearch?.addEventListener("input", () => loadMountAdminPage(1));
  els.relaySearch?.addEventListener("input", () => loadRelayPage(1));
  els.userPrevBtn?.addEventListener("click", () => loadUserPage((state.userPageResult?.page || 1) - 1));
  els.userNextBtn?.addEventListener("click", () => loadUserPage((state.userPageResult?.page || 1) + 1));
  els.mountPrevBtn?.addEventListener("click", () => loadMountAdminPage((state.mountAdminPageResult?.page || 1) - 1));
  els.mountNextBtn?.addEventListener("click", () => loadMountAdminPage((state.mountAdminPageResult?.page || 1) + 1));
  els.relayPrevBtn?.addEventListener("click", () => loadRelayPage((state.relayPageResult?.page || 1) - 1));
  els.relayNextBtn?.addEventListener("click", () => loadRelayPage((state.relayPageResult?.page || 1) + 1));

  els.blockForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(els.blockForm);
    await api.saveBlock({ ip: form.get("ip"), expiresAt: form.get("expiresAt") || "永久", reason: form.get("reason") });
    els.blockForm.reset();
    await reload();
  });

  els.limitsForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    await api.saveLimits({
      maxClients: Number(els.limitsForm.maxClients.value || 0),
      maxSources: Number(els.limitsForm.maxSources.value || 0),
      maxPending: Number(els.limitsForm.maxPending.value || 0),
      maxConnections: Number(els.limitsForm.maxConnections.value || 0),
    });
    await reload();
  });

  els.adminProfileForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(els.adminProfileForm);
    try {
      await api.saveAdminProfile({
        enabled: String(form.get("enabled")) === "true",
        username: String(form.get("username") || "").trim(),
        password: String(form.get("password") || ""),
      });
      await reload();
      setState({ message: "管理员设置已保存" });
    } catch (error) {
      setState({ message: error.message || "保存管理员设置失败" });
    }
  });

  els.authSettingsForm?.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(els.authSettingsForm);
    try {
      await api.saveAuthSettings({
        oidc: {
          enabled: String(form.get("oidcEnabled")) === "true",
          provider: String(form.get("provider") || "pocketid").trim(),
          issuerURL: String(form.get("issuerURL") || "").trim(),
          clientID: String(form.get("clientID") || "").trim(),
          clientSecret: String(form.get("clientSecret") || ""),
          redirectURL: String(form.get("redirectURL") || "").trim(),
        },
      });
      await reload();
      setState({ message: "登录方式已保存" });
    } catch (error) {
      setState({ message: error.message || "保存登录方式失败" });
    }
  });

  els.mountForm.addEventListener("submit", async (event) => {
    event.preventDefault();
    const form = new FormData(els.mountForm);
    const payload = {
      name: String(form.get("name") || "").trim(),
      description: String(form.get("description") || "").trim(),
      supportedConstellations: String(form.get("constellations") || "").split(",").map((item) => item.trim()).filter(Boolean),
      rtcmMessages: String(form.get("rtcmMessages") || "").split(",").map((item) => item.trim()).filter(Boolean),
      decodeCandidate: String(form.get("decodeCandidate")) === "true",
      position: null,
    };
    const latitude = String(form.get("latitude") || "").trim();
    const longitude = String(form.get("longitude") || "").trim();
    if (latitude !== "" && longitude !== "") {
      payload.position = {
        latitude: Number(latitude),
        longitude: Number(longitude),
      };
    }
    await api.saveMount(payload);
    els.mountForm.reset();
    await reload();
  });

  initializeAuth();
})();
