use plotters::prelude::*;
use mmoldb_db::db::Progress;

pub fn plot(progress: Progress) -> Result<String, DrawingAreaErrorKind<std::io::Error>> {
    const WIDTH: u32 = 800;
    const HEIGHT: u32 = 200;

    let mut svg_content = String::new();
    // Need a new scope because the plot stuff has to be dropped before we can return svg_content
    {
        let drawing_area = SVGBackend::with_string(&mut svg_content, (WIDTH, HEIGHT)).into_drawing_area();
        drawing_area.fill(&RGBColor(20, 20, 20))?;

        let max_bucket_size = progress.buckets.iter().map(|(_, count)| count).max().cloned().unwrap_or(0);

        let mut chart = ChartBuilder::on(&drawing_area)
            .margin(5)
            .x_label_area_size(30)
            .y_label_area_size(50)
            .caption("Game ingest status", ("sans-serif", 20, &RGBColor(255, 255, 255)))
            .build_cartesian_2d(progress.history_start..progress.history_end, (0..max_bucket_size).log_scale())?;

        chart.configure_mesh()
            .disable_mesh()
            .axis_style(&WHITE)
            .label_style(&WHITE)
            .x_label_formatter(&|v| format!("{}", v.date_naive()))
            .x_labels(10)
            .draw()?;

        let color = RGBColor(200, 200, 200);
        chart
            .draw_series(
                AreaSeries::new(progress.buckets, 0, color.mix(0.2))
                    .border_style(color)
            )?;

        drawing_area.present()?;
    }

    Ok(svg_content)
}