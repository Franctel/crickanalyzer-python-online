(function () {
  // ====== Detect selected match safely ======
  const match =
    window.MULTIDAY_DASHBOARD_CONFIG?.matchName ||
    window.selected_match ||
    document.querySelector('meta[name="selected_match"]')?.content ||
    "";

  // ✅ Flask JSON endpoint (matches @apps.route("/apps/multiday/json/<string:match_name>"))
  const ENDPOINT = `/apps/multiday/json/${encodeURIComponent(match)}`;
  console.log("📊 Fetching data from:", ENDPOINT);

  // Chart references
  let wormChart, dayRunsWktsChart, timelineChart, splitChart, workloadChart, pressureChart;

  // Theme-safe color palette
  const COLORS = ["#0ea5e9", "#22c55e", "#f59e0b", "#ef4444", "#8b5cf6", "#14b8a6"];

  // ---------- BUILD ----------
  function buildCharts(payload) {
    const inns = payload.innings || [];
    if (!inns.length) {
      console.warn("⚠️ No innings data available for this match.");
      return;
    }

    // 🟦 1) Worm Chart
    const wormSeries = inns.map((inn) => ({
      name: `Inns ${inn.inn_no}`,
      data: inn.worm?.values || [],
    }));
    const wormLabels = inns[0]?.worm?.labels || [];
    wormChart = new ApexCharts(document.querySelector("#chart_worm"), {
      chart: { type: "area", height: 300, animations: { enabled: true } },
      series: wormSeries,
      xaxis: { categories: wormLabels, title: { text: "Over.Ball" } },
      dataLabels: { enabled: false },
      stroke: { curve: "smooth", width: 2 },
      colors: COLORS,
      tooltip: { shared: true },
    });
    wormChart.render();

    // Collect unique day numbers
    const daySet = new Set();
    inns.forEach((inn) => inn.per_day?.forEach((r) => daySet.add(r.scrM_DayNo)));
    const dayCats = Array.from(daySet)
      .sort((a, b) => a - b)
      .map((d) => `Day ${d}`);

    // 🟩 2) Runs & Wickets by Day
    const runsSeries = inns.map((inn) => ({
      name: `Runs (Inns ${inn.inn_no})`,
      type: "column",
      data: dayCats.map((lbl) => {
        const d = parseInt(lbl.replace("Day ", ""));
        const row = inn.per_day?.find((x) => x.scrM_DayNo === d);
        return row ? row.runs : 0;
      }),
    }));

    const wktsSeries = inns.map((inn) => ({
      name: `Wkts (Inns ${inn.inn_no})`,
      type: "line",
      data: dayCats.map((lbl) => {
        const d = parseInt(lbl.replace("Day ", ""));
        const row = inn.per_day?.find((x) => x.scrM_DayNo === d);
        return row ? row.wkts : 0;
      }),
    }));

    dayRunsWktsChart = new ApexCharts(document.querySelector("#chart_day_runs_wkts"), {
      chart: { height: 300, stacked: false, toolbar: { show: false } },
      series: [...runsSeries, ...wktsSeries],
      xaxis: { categories: dayCats },
      dataLabels: { enabled: false },
      stroke: { width: [0, 0, 0, 3, 3, 3] },
      yaxis: [{ title: { text: "Runs" } }, { opposite: true, title: { text: "Wickets" } }],
      colors: COLORS,
    });
    dayRunsWktsChart.render();

    // 🟨 3) Session Timeline
    const timelineSeries = inns.map((inn) => {
      const data = (inn.per_session || [])
        .sort(
          (a, b) =>
            a.scrM_DayNo - b.scrM_DayNo || a.scrM_SessionNo - b.scrM_SessionNo
        )
        .map((r) => ({
          x: `D${r.scrM_DayNo}-S${r.scrM_SessionNo}`,
          y: [r.scrM_DayNo * 10 + r.scrM_SessionNo, r.scrM_DayNo * 10 + r.scrM_SessionNo + 1],
        }));
      return { name: `Inns ${inn.inn_no}`, data };
    });

    timelineChart = new ApexCharts(document.querySelector("#chart_session_timeline"), {
      chart: { type: "rangeBar", height: 320 },
      plotOptions: { bar: { horizontal: true, rangeBarGroupRows: true } },
      series: timelineSeries,
      dataLabels: { enabled: false },
      colors: COLORS,
      xaxis: { labels: { show: false }, title: { text: "Sessions" } },
    });
    timelineChart.render();

    // 🟧 4) Scoring Split
    const splitNames = ["dots", "ones", "twos", "threes", "fours", "sixes", "extras"];
    const splitSeries = splitNames.map((name) => ({
      name: name.toUpperCase(),
      data: dayCats.map((lbl) => {
        const d = parseInt(lbl.replace("Day ", ""));
        let total = 0;
        inns.forEach((inn) => {
          const row = inn.score_split?.find((x) => x.scrM_DayNo === d);
          total += row ? row[name] || 0 : 0;
        });
        return total;
      }),
    }));

    splitChart = new ApexCharts(document.querySelector("#chart_scoring_split"), {
      chart: { type: "bar", height: 330, stacked: true, stackType: "100%" },
      series: splitSeries,
      xaxis: { categories: dayCats },
      dataLabels: { enabled: false },
      legend: { position: "top" },
      colors: COLORS,
    });
    splitChart.render();

    // 🟪 5) Bowler Workloads
    const totals = {};
    inns.forEach((inn) => {
      (inn.bowling_workloads || []).forEach((r) => {
        const k = r.scrM_PlayMIdBowlerName || "Unknown";
        totals[k] = (totals[k] || 0) + (r.overs || 0);
      });
    });

    const top = Object.entries(totals)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 6)
      .map(([name]) => name);

    const dayIndices = dayCats.map((lbl) => parseInt(lbl.replace("Day ", "")));
    const rbSeries = dayIndices.map((dayNo) => {
      let sum = 0;
      inns.forEach((inn) => {
        (inn.bowling_workloads || []).forEach((r) => {
          if (
            r.scrM_DayNo === dayNo &&
            top.includes(r.scrM_PlayMIdBowlerName || "Unknown")
          ) {
            sum += r.overs || 0;
          }
        });
      });
      return sum;
    });

    workloadChart = new ApexCharts(document.querySelector("#chart_bowler_workload"), {
      chart: { type: "radialBar", height: 300 },
      series: rbSeries,
      labels: dayCats,
      colors: COLORS,
      plotOptions: {
        radialBar: {
          dataLabels: {
            total: { show: true, label: "Overs (Top Bowlers)" },
          },
        },
      },
    });
    workloadChart.render();

    // 🟥 6) Over Pressure
    const pressureSeries = dayCats.map((lbl) => {
      const day = parseInt(lbl.replace("Day ", ""));
      const pts = [];
      inns.forEach((inn) => {
        (inn.over_pressure || [])
          .filter((r) => r.scrM_DayNo === day)
          .forEach((r) =>
            pts.push([r.scrM_OverNo, r.rr, Math.max(1, r.wkts * 6)])
          );
      });
      return { name: lbl, data: pts };
    });

    pressureChart = new ApexCharts(document.querySelector("#chart_over_pressure"), {
      chart: { type: "bubble", height: 300 },
      series: pressureSeries,
      xaxis: { title: { text: "Over" }, tickAmount: 10 },
      yaxis: { title: { text: "Runs in Over" } },
      dataLabels: { enabled: false },
      colors: COLORS,
    });
    pressureChart.render();
  }

  // ---------- UPDATE ----------
  function updateCharts(payload) {
    if (!payload?.innings?.length) return;
    const inns = payload.innings;

    const wormSeries = inns.map((inn) => ({
      name: `Inns ${inn.inn_no}`,
      data: inn.worm?.values || [],
    }));
    const wormLabels = inns[0]?.worm?.labels || [];
    wormChart.updateOptions({ xaxis: { categories: wormLabels } });
    wormChart.updateSeries(wormSeries);

    const daySet = new Set();
    inns.forEach((inn) => inn.per_day?.forEach((r) => daySet.add(r.scrM_DayNo)));
    const dayCats = Array.from(daySet)
      .sort((a, b) => a - b)
      .map((d) => `Day ${d}`);

    const runsSeries = inns.map((inn) => ({
      name: `Runs (Inns ${inn.inn_no})`,
      data: dayCats.map((lbl) => {
        const d = parseInt(lbl.replace("Day ", ""));
        const row = inn.per_day?.find((x) => x.scrM_DayNo === d);
        return row ? row.runs : 0;
      }),
    }));

    const wktsSeries = inns.map((inn) => ({
      name: `Wkts (Inns ${inn.inn_no})`,
      data: dayCats.map((lbl) => {
        const d = parseInt(lbl.replace("Day ", ""));
        const row = inn.per_day?.find((x) => x.scrM_DayNo === d);
        return row ? row.wkts : 0;
      }),
    }));

    dayRunsWktsChart.updateOptions({ xaxis: { categories: dayCats } });
    dayRunsWktsChart.updateSeries([...runsSeries, ...wktsSeries]);

    const timelineSeries = inns.map((inn) => {
      const data = (inn.per_session || []).map((r) => ({
        x: `D${r.scrM_DayNo}-S${r.scrM_SessionNo}`,
        y: [r.scrM_DayNo * 10 + r.scrM_SessionNo, r.scrM_DayNo * 10 + r.scrM_SessionNo + 1],
      }));
      return { name: `Inns ${inn.inn_no}`, data };
    });
    timelineChart.updateSeries(timelineSeries);

    const splitNames = ["dots", "ones", "twos", "threes", "fours", "sixes", "extras"];
    const splitSeries = splitNames.map((name) => ({
      name: name.toUpperCase(),
      data: dayCats.map((lbl) => {
        const d = parseInt(lbl.replace("Day ", ""));
        let total = 0;
        inns.forEach((inn) => {
          const row = inn.score_split?.find((x) => x.scrM_DayNo === d);
          total += row ? row[name] || 0 : 0;
        });
        return total;
      }),
    }));
    splitChart.updateOptions({ xaxis: { categories: dayCats } });
    splitChart.updateSeries(splitSeries);

    const totals = {};
    inns.forEach((inn) => {
      (inn.bowling_workloads || []).forEach((r) => {
        const k = r.scrM_PlayMIdBowlerName || "Unknown";
        totals[k] = (totals[k] || 0) + (r.overs || 0);
      });
    });

    const top = Object.entries(totals)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 6)
      .map(([name]) => name);
    const dayIndices = dayCats.map((lbl) => parseInt(lbl.replace("Day ", "")));
    const rbSeries = dayIndices.map((dayNo) => {
      let sum = 0;
      inns.forEach((inn) => {
        (inn.bowling_workloads || []).forEach((r) => {
          if (
            r.scrM_DayNo === dayNo &&
            top.includes(r.scrM_PlayMIdBowlerName || "Unknown")
          ) {
            sum += r.overs || 0;
          }
        });
      });
      return sum;
    });
    workloadChart.updateOptions({ labels: dayCats });
    workloadChart.updateSeries(rbSeries);

    const pressureSeries = dayCats.map((lbl) => {
      const day = parseInt(lbl.replace("Day ", ""));
      const pts = [];
      inns.forEach((inn) => {
        (inn.over_pressure || [])
          .filter((r) => r.scrM_DayNo === day)
          .forEach((r) =>
            pts.push([r.scrM_OverNo, r.rr, Math.max(1, r.wkts * 6)])
          );
      });
      return { name: lbl, data: pts };
    });
    pressureChart.updateSeries(pressureSeries);
  }

  // ---------- FETCH ----------
  function fetchAndDraw(initial = false) {
    fetch(ENDPOINT + "?force=0")
      .then((r) => r.json())
      .then((json) => {
        if (initial) buildCharts(json);
        else updateCharts(json);
      })
      .catch((err) => console.error("❌ Error fetching multiday data:", err));
  }

  // Initial draw + auto-refresh every 30 seconds
  fetchAndDraw(true);
  setInterval(() => fetchAndDraw(false), 30000);
})();
