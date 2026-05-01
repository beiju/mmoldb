use plotters::prelude::*;
use mmoldb_db::db::{Progress, ProgressBucket};

pub fn plot(kind_label: &str, progress: Progress) -> Result<String, DrawingAreaErrorKind<std::io::Error>> {
    const WIDTH: u32 = 800;
    const HEIGHT: u32 = 200;

    let mut svg_content = String::new();
    // Need a new scope because the plot stuff has to be dropped before we can return svg_content
    {
        let drawing_area = SVGBackend::with_string(&mut svg_content, (WIDTH, HEIGHT)).into_drawing_area();
        drawing_area.fill(&RGBColor(20, 20, 20))?;

        let max_bucket_size = progress.buckets
            .iter()
            .map(|bucket| bucket.raw_total)
            .max()
            .unwrap_or(0);

        let mut chart = ChartBuilder::on(&drawing_area)
            .margin(5)
            .x_label_area_size(30)
            .y_label_area_size(50)
            .caption(format!("{kind_label} ingest progress"), ("sans-serif", 20, &RGBColor(255, 255, 255)))
            .build_cartesian_2d(progress.history_start..progress.history_end, (0..max_bucket_size).log_scale())?;

        chart.configure_mesh()
            .disable_mesh()
            .axis_style(&WHITE)
            .label_style(&WHITE)
            .x_label_formatter(&|v| format!("{}", v.date_naive()))
            .x_labels(10)
            .draw()?;
        fn raw_total(bucket: &ProgressBucket) -> i64 { bucket.raw_total }
        fn processed_total(bucket: &ProgressBucket) -> i64 { bucket.processed_total }

        let lines = [
            ("Raw", RGBColor(200, 200, 200), raw_total as fn(&ProgressBucket) -> i64),
            ("Processed", BLUE, processed_total as fn(&ProgressBucket) -> i64),
        ];

        for (label, color, extractor) in lines {
            let mut buckets_for_line: Vec<_> = progress.buckets
                .iter()
                .map(|bucket| (bucket.bucket_start, extractor(bucket)))
                .collect();
            // Don't draw points at zero after the last nonzero, to make it easier to
            // see where the data stops
            let Some(last_nonzero) = buckets_for_line.iter().rposition(|(_, count)| *count > 0) else {
                // If this is None, that means there's no nonzero values so there's nothing to plot
                continue;
            };
            buckets_for_line.truncate(last_nonzero + 1);

            chart
                .draw_series(
                    AreaSeries::new(buckets_for_line, 0, color.mix(0.2))
                        .border_style(&color)
                )?
                .label(label)
                .legend(move |(x,y)| {
                    let half_size = 6;
                    // See docs for legend for why the coordinates are like this
                    Rectangle::new([(x, y - half_size), (x + 2 * half_size, y + half_size)], color.filled())
                });
        }

        chart.configure_series_labels()
            .position(SeriesLabelPosition::UpperLeft)
            .margin(10)
            .legend_area_size(20)
            .border_style(BLACK)
            .background_style(&RGBColor(30, 30, 30))
            .label_font(("sans-serif", 15, &WHITE))
            .draw()?;

        drawing_area.present()?;
    }

    Ok(svg_content)
}