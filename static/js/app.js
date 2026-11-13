const { createApp } = Vue;

createApp({
  data() {
    return {
      me: "",
      items: [],
      current: null,
      spec: null,
      methods: [],
      specToday: "",
      extractingAny: false,
      busy: false,
      msg: "",
      alert: "",
      specMsg: "",
      specForm: { method: "source_max_ts", note: "" },
      extractForm: { mode: "incremental", reason: "" },
    };
  },
  async mounted() {
    const me = await fetch("/api/me");
    if (!me.ok) {
      location.href = "/login";
      return;
    }
    this.me = (await me.json()).user;
    await Promise.all([this.loadSpec(), this.refresh()]);
  },
  methods: {
    tagClass(p) {
      if (p.extracting) return "warn";
      const s = p.status || "";
      if (s === "已落库") return "ok";
      if (s === "已抽取待加载") return "warn";
      return "";
    },
    async loadSpec() {
      const res = await fetch("/api/spec");
      if (!res.ok) return;
      const data = await res.json();
      this.spec = data.spec;
      this.methods = data.methods || [];
      this.specToday = data.today || "";
      this.extractingAny = !!data.extracting;
    },
    async refresh() {
      const res = await fetch("/api/pipelines");
      if (!res.ok) return;
      this.items = (await res.json()).items || [];
    },
    async open(id) {
      const res = await fetch(`/api/pipelines/${id}`);
      if (!res.ok) return;
      this.current = await res.json();
      this.msg = "";
    },
    lightUp(error) {
      // 看板同步点亮：顶部告警条 + 列表读数刷新
      this.alert = error;
      this.refresh();
    },
    async pinSpec() {
      this.busy = true;
      this.specMsg = "";
      this.alert = "";
      try {
        const res = await fetch("/api/spec", {
          method: "PUT",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(this.specForm),
        });
        const data = await res.json();
        if (!res.ok) {
          this.specMsg = data.error || "钉死失败";
          this.lightUp(this.specMsg);
          return;
        }
        this.spec = data.spec;
        await this.refresh();
      } finally {
        this.busy = false;
      }
    },
    async extract() {
      if (!this.current) return;
      this.busy = true;
      this.msg = "";
      this.alert = "";
      try {
        const res = await fetch(`/api/pipelines/${this.current.id}/extract`, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            mode: this.extractForm.mode,
            method: this.spec ? this.spec.method : "",
            reason: this.extractForm.reason,
          }),
        });
        const data = await res.json();
        if (!res.ok) {
          this.lightUp(data.error || "抽取失败");
          return;
        }
        this.msg = `抽取完成：收入 ${data.staged} 行，水位 ${data.watermark_display}`;
        await this.open(this.current.id);
        await this.refresh();
      } finally {
        this.busy = false;
      }
    },
    async load() {
      if (!this.current) return;
      this.busy = true;
      this.msg = "";
      this.alert = "";
      try {
        const res = await fetch(`/api/pipelines/${this.current.id}/load`, {
          method: "POST",
        });
        const data = await res.json();
        if (!res.ok) {
          this.lightUp(data.error || "落库失败");
          return;
        }
        this.msg =
          data.status === "已落库"
            ? `落库完成，累计落库 ${data.loaded} 行`
            : "当前状态不能落库，请先抽取";
        await this.open(this.current.id);
        await this.refresh();
      } finally {
        this.busy = false;
      }
    },
    async logout() {
      await fetch("/api/logout", { method: "POST" });
      location.href = "/login";
    },
  },
}).mount("#app");
