document.addEventListener('renderAreawiseReport', function () {
    const loader = document.getElementById('areawise-loader');
    const content = document.getElementById('areawise-content');

    if (!content || !loader) return;

    // --- Show Loader, Hide Content ---
    loader.classList.remove('hidden');
    content.classList.add('hidden');

    // --- Sorting Striker Cards ---
    const strikersContainer = document.getElementById('strikers-container');
    if (strikersContainer) {
        const strikerCards = Array.from(strikersContainer.getElementsByClassName('striker-card'));
        strikerCards.sort((a, b) => (parseInt(b.dataset.totalRuns, 10) || 0) - (parseInt(a.dataset.totalRuns, 10) || 0));
        strikerCards.forEach(card => strikersContainer.appendChild(card));
    }

    // --- Chart and Filter Logic ---
    const filtersContainer = document.getElementById('areawise-filters');
    const mainChartEl = document.getElementById('areawise-bar-chart');

    if (!filtersContainer || !mainChartEl) {
        if(loader) loader.classList.add('hidden');
        if(content) content.classList.remove('hidden');
        return;
    }

    try {
        // --- Generate Filter Buttons ---
        const mainChartData = JSON.parse(mainChartEl.dataset.chartData);
        const areas = mainChartData.labels || [];
        let buttonsHTML = '<button data-filter="all" class="areawise-filter-btn active-filter px-3 py-1 text-xs font-medium rounded-md bg-custom-500 text-white">All</button>';
        areas.forEach(area => {
            buttonsHTML += `<button data-filter="${area}" class="areawise-filter-btn px-3 py-1 text-xs font-medium rounded-md bg-slate-100 text-slate-700 dark:bg-zink-600 dark:text-zink-100">${area}</button>`;
        });
        filtersContainer.innerHTML = buttonsHTML;

        // --- Filter Event Listener ---
        filtersContainer.addEventListener('click', function(event) {
            const button = event.target.closest('.areawise-filter-btn');
            if (!button) return;

            // Update button styles
            filtersContainer.querySelectorAll('.areawise-filter-btn').forEach(btn => {
                btn.classList.remove('active-filter', 'bg-custom-500', 'text-white');
                btn.classList.add('bg-slate-100', 'text-slate-700', 'dark:bg-zink-600', 'dark:text-zink-100');
            });
            button.classList.add('active-filter', 'bg-custom-500', 'text-white');
            button.classList.remove('bg-slate-100', 'text-slate-700', 'dark:bg-zink-600', 'dark:text-zink-100');
            
            const filter = button.dataset.filter;
            
            // Process filtering
            document.querySelectorAll('.striker-card').forEach(card => {
                let hasVisibleRows = false;
                card.querySelectorAll('tbody tr').forEach(row => {
                    const areaName = row.dataset.areaName;
                    const shouldBeVisible = (filter === 'all' || areaName === filter);
                    row.classList.toggle('hidden', !shouldBeVisible);
                    if (shouldBeVisible) {
                        hasVisibleRows = true;
                    }
                });
                card.classList.toggle('hidden', !hasVisibleRows);
            });
        });

    } catch (e) {
        console.error("Error setting up areawise filters:", e);
    }

    // --- Chart Rendering ---
    const colorsMap = {
        ones: "rgba(59,130,246,0.7)", twos: "rgba(139,92,246,0.7)",
        fours: "rgba(34,197,94,0.7)", sixes: "rgba(239,68,68,0.7)",
        runs: "rgba(234,179,8,0.7)"
    };
    
    // Main Chart
    try {
        const chartData = JSON.parse(mainChartEl.dataset.chartData);
        const mainChartOptions = {
            chart: { 
                type: "bar", 
                height: chartData.height || 350, 
                toolbar: { show: false },
                events: {
                    mounted: function(chartContext, config) {
                        // This event ensures the chart is fully rendered
                        loader.classList.add('hidden');
                        content.classList.remove('hidden');
                    }
                }
            },
            plotOptions: { 
                bar: { 
                    horizontal: true, 
                    barHeight: "60%", 
                    dataLabels: { position: "center" } 
                } 
            },
            dataLabels: { 
                enabled: true, 
                textAnchor: 'middle',
                style: { colors: ["#fff"], fontSize: "13px", fontWeight: "bold" }, 
                dropShadow: { enabled: true, top: 1, left: 1, blur: 1, opacity: 0.5 } 
            },
            colors: ["rgba(0, 143, 251, 0.85)", "rgba(0, 227, 150, 0.85)", "rgba(254, 176, 25, 0.85)", "rgba(255, 69, 96, 0.85)"],
            series: chartData.series || [], 
            xaxis: { categories: chartData.labels || [] },
            title: { text: chartData.title || "", align: "center" }, 
            legend: { position: "top" }
        };
        new ApexCharts(mainChartEl, mainChartOptions).render();
    } catch (e) { 
        console.error("Error rendering main chart", e);
        loader.classList.add('hidden');
        content.classList.remove('hidden');
    }

    // --- Lazy Load Inline Charts ---
    const lazyRenderChart = (entries, observer) => {
        entries.forEach(entry => {
            if (entry.isIntersecting) {
                const el = entry.target;
                try {
                    const chartData = JSON.parse(el.dataset.chartData);
                    const type = el.dataset.type;
                    new ApexCharts(el, {
                        chart: { type: "bar", height: 24, sparkline: { enabled: true } },
                        plotOptions: { bar: { horizontal: true, columnWidth: "80%", dataLabels: { position: "center" } } },
                        colors: [colorsMap[type] || "rgba(100,100,100,0.6)"], series: chartData.series,
                        dataLabels: { enabled: true, formatter: (val) => val, style: { colors: ["#fff"], fontSize: "10px", fontWeight: "bold" } },
                        tooltip: { enabled: false },
                        xaxis: { labels: { show: false }, axisBorder: { show: false }, axisTicks: { show: false } },
                        yaxis: { labels: { show: false } }
                    }).render();
                } catch (e) { console.error("Error rendering shot-bar", e); }
                observer.unobserve(el);
            }
        });
    };

    const observer = new IntersectionObserver(lazyRenderChart, {
        root: null,
        rootMargin: '0px',
        threshold: 0.1
    });

    document.querySelectorAll(".shot-bar").forEach(el => observer.observe(el));
});
