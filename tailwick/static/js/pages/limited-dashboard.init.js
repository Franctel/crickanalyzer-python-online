(function () {
  // ====== Detect selected match safely ======
  const match =
    window.LIMITED_DASHBOARD_CONFIG?.matchName ||
    window.selected_match ||
    document.querySelector('meta[name="selected_match"]')?.content ||
    "";

  const ENDPOINT = `/apps/limited-dashboard/json/${encodeURIComponent(match)}`;
  console.log("📊 Fetching Limited Overs Dashboard data from:", ENDPOINT);

  let wormChart, rrChart, phaseChart, splitChart, econChart, pressureChart;
  const COLORS = ["#0ea5e9", "#22c55e", "#f59e0b", "#ef4444", "#8b5cf6", "#14b8a6"];

  // ---------- BUILD ----------
  function buildCharts(payload) {
    const inns = payload.innings || [];
    if (!inns.length) {
      console.warn("⚠️ No innings data available for this match.");
      return;
    }

    // 🟦 1) Worm Chart (Cumulative Runs)
    const wormSeries = inns.map((inn) => ({
      name: `Inns ${inn.inn_no}`,
      data: inn.worm?.values || [],
    }));
    const wormLabels = inns[0]?.worm?.labels || [];
    wormChart = new ApexCharts(document.querySelector("#chart_worm"), {
      chart: { type: "area", height: 300 },
      series: wormSeries,
      xaxis: { categories: wormLabels, title: { text: "Over.Ball" } },
      stroke: { curve: "smooth", width: 2 },
      dataLabels: { enabled: false },
      colors: COLORS,
      tooltip: { shared: true },
    });
    wormChart.render();

    // 🟩 2) Run Rate by Over
    const rrSeries = inns.map((inn) => ({
      name: `Inns ${inn.inn_no}`,
      data: inn.run_rate?.map((r) => r.runs || 0) || [],
    }));
    const rrLabels = inns[0]?.run_rate?.map((r) => r.scrM_OverNo) || [];
    rrChart = new ApexCharts(document.querySelector("#chart_run_rate"), {
      chart: { type: "line", height: 300 },
      series: rrSeries,
      xaxis: { categories: rrLabels, title: { text: "Over Number" } },
      yaxis: { title: { text: "Runs per Over" } },
      stroke: { curve: "smooth", width: 2 },
      colors: COLORS,
      dataLabels: { enabled: false },
    });
    rrChart.render();

    // 🟨 3) Phase Summary (Powerplay / Middle / Death)
    const phaseNames = ["Powerplay", "Middle Overs", "Death Overs"];
    const phaseSeries = phaseNames.map((ph) => ({
      name: ph,
      data: inns.map((inn) => {
        const row = inn.phase_summary?.find((r) => r.phase === ph);
        return row ? row.runs : 0;
      }),
    }));
    const innsCats = inns.map((inn) => `Inns ${inn.inn_no}`);
    phaseChart = new ApexCharts(document.querySelector("#chart_phase_summary"), {
      chart: { type: "bar", height: 300, stacked: true },
      series: phaseSeries,
      xaxis: { categories: innsCats },
      dataLabels: { enabled: false },
      legend: { position: "top" },
      colors: COLORS,
    });
    phaseChart.render();

    // 🟧 4) Boundary Split
    const splitCats = ["Dots", "Ones", "Twos", "Threes", "Fours", "Sixes"];
    const splitSeries = inns.map((inn) => ({
      name: `Inns ${inn.inn_no}`,
      data: [
        inn.boundary_split?.dots || 0,
        inn.boundary_split?.ones || 0,
        inn.boundary_split?.twos || 0,
        inn.boundary_split?.threes || 0,
        inn.boundary_split?.fours || 0,
        inn.boundary_split?.sixes || 0,
      ],
    }));
    splitChart = new ApexCharts(document.querySelector("#chart_boundary_split"), {
      chart: { type: "bar", height: 330, stacked: false },
      series: splitSeries,
      xaxis: { categories: splitCats },
      dataLabels: { enabled: false },
      colors: COLORS,
      legend: { position: "top" },
    });
    splitChart.render();

    // 🟪 5) Bowler Economy (Top 5 Bowlers)
    const allBowlers = inns.flatMap((inn) => inn.bowler_economy || []);
    const topBowlers = allBowlers.slice(0, 5);
    econChart = new ApexCharts(document.querySelector("#chart_bowler_econ"), {
      chart: { type: "bar", height: 300 },
      series: [{ name: "Economy", data: topBowlers.map((b) => b.econ.toFixed(2)) }],
      xaxis: { categories: topBowlers.map((b) => b.scrM_PlayMIdBowlerName || "Unknown") },
      colors: COLORS,
      plotOptions: { bar: { horizontal: true } },
      dataLabels: { enabled: true },
    });
    econChart.render();

    // 🟥 6) Over Pressure (Bubble: Over vs Runs, size=Wickets)
    const pressureSeries = inns.map((inn) => ({
      name: `Inns ${inn.inn_no}`,
      data: (inn.over_pressure || []).map((r) => ({
        x: r.x || r.scrM_OverNo,
        y: r.y || r.runs,
        z: r.z || Math.max(6, r.wkts * 8),
      })),
    }));

    pressureChart = new ApexCharts(document.querySelector("#chart_over_pressure"), {
      chart: { type: "bubble", height: 300 },
      series: pressureSeries,
      xaxis: { title: { text: "Over No" } },
      yaxis: { title: { text: "Runs in Over" } },
      colors: COLORS,
      dataLabels: { enabled: false },
      tooltip: {
        y: { formatter: (val) => `${val} runs` },
        z: { formatter: (val) => `${Math.round(val / 8)} wickets` },
      },
    });
    pressureChart.render();
  }

  // ---------- FETCH ----------
  function fetchAndDraw(initial = false) {
    fetch(ENDPOINT + "?force=0")
      .then((r) => r.json())
      .then((json) => {
        if (initial) buildCharts(json);
      })
      .catch((err) => console.error("❌ Error fetching limited overs data:", err));
  }

  // Initial draw + auto-refresh every 30s
  fetchAndDraw(true);
  setInterval(() => fetchAndDraw(false), 30000);
})();
