package com.nexagres.advisor.report;

import com.nexagres.advisor.catalog.CatalogSnapshot;
import com.nexagres.advisor.core.ConnectionRecord;
import com.nexagres.advisor.score.MigrationScorer.MigrationScoreReport;
import com.nexagres.advisor.score.MigrationScorer.ScoreFinding;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.pdmodel.PDPage;
import org.apache.pdfbox.pdmodel.PDPageContentStream;
import org.apache.pdfbox.pdmodel.common.PDRectangle;
import org.apache.pdfbox.pdmodel.font.PDFont;
import org.apache.pdfbox.pdmodel.font.PDType1Font;
import org.apache.pdfbox.pdmodel.font.Standard14Fonts;

/**
 * Builds the Findings tab's "Download report" PDF -- a live snapshot generated on demand from
 * whatever {@link CatalogSnapshot}/{@link MigrationScoreReport} the caller just produced (see
 * {@code ConnectionsRoute#runReport}), never a cached copy from an earlier visit. Deliberately
 * scoped to Findings only (score, tier, feature inventory, scored findings) -- not Workload,
 * Objects, or Parameters -- per the project decision that this is a migration-assessment summary
 * a stakeholder would actually read, not a full data dump.
 *
 * <p>Uses PDFBox directly (low-level content-stream drawing, manual text wrapping/pagination)
 * rather than a templating layer -- proportionate to one fixed report shape, not a library of
 * report types.
 */
public class FindingsReportGenerator {

    private static final float MARGIN = 50;
    private static final float PAGE_WIDTH = PDRectangle.LETTER.getWidth();
    private static final float PAGE_HEIGHT = PDRectangle.LETTER.getHeight();
    private static final float CONTENT_WIDTH = PAGE_WIDTH - 2 * MARGIN;

    public byte[] generate(ConnectionRecord connection, CatalogSnapshot snapshot, MigrationScoreReport score) throws IOException {
        try (PDDocument document = new PDDocument()) {
            Writer w = new Writer(document);

            w.heading("Nexagres Advisor -- Migration Assessment", 20);
            w.line(connection.name, 12, true);
            w.line(connection.jdbcUrl, 10, false, GRAY);
            w.line("Generated " + java.time.OffsetDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss 'UTC'")), 9, false, GRAY);
            w.gap(14);

            w.heading("Overall complexity: " + score.tier.split(" -- ")[0], 14);
            w.line(score.tier.contains(" -- ") ? score.tier.split(" -- ", 2)[1] : "", 11, false);
            w.line("Total difficulty score: " + score.totalScore, 11, true);
            if (snapshot.sourceVersion != null) {
                w.line(snapshot.sourceVersion, 9, false, GRAY);
            }
            w.gap(14);

            if (!score.warnings.isEmpty()) {
                w.heading("Warnings", 12);
                for (String warning : score.warnings) {
                    w.bullet(warning, 10);
                }
                w.gap(10);
            }

            w.heading("Feature inventory", 12);
            List<String[]> inventory = new ArrayList<>();
            inventory.add(new String[]{"Tables", String.valueOf(snapshot.tableCount)});
            inventory.add(new String[]{"Views", String.valueOf(snapshot.viewCount)});
            inventory.add(new String[]{"Materialized views", String.valueOf(snapshot.materializedViewCount)});
            inventory.add(new String[]{"Sequences", String.valueOf(snapshot.sequenceCount)});
            inventory.add(new String[]{"Triggers (simple / complex)", snapshot.simpleTriggerCount + " / " + snapshot.complexTriggerCount});
            inventory.add(new String[]{"Packages", String.valueOf(snapshot.packageCount)});
            inventory.add(new String[]{"Standalone procedures / functions", snapshot.standaloneProcedureCount + " / " + snapshot.standaloneFunctionCount});
            inventory.add(new String[]{"Database links", String.valueOf(snapshot.dbLinkCount)});
            inventory.add(new String[]{"Scheduled jobs", String.valueOf(snapshot.scheduledJobCount)});
            inventory.add(new String[]{"Partitioned tables", String.valueOf(snapshot.partitionedTableCount)});
            w.table(new String[]{"Feature", "Count"}, inventory, new float[]{0.7f, 0.3f});
            w.gap(14);

            w.heading("Migration complexity by item", 12);
            List<ScoreFinding> sorted = new ArrayList<>(score.findings);
            sorted.sort((a, b) -> Integer.compare(b.points(), a.points()));
            if (sorted.isEmpty()) {
                w.line("No difficulty-scoring findings -- looks like a clean schema+data migration.", 10);
            }
            for (ScoreFinding f : sorted) {
                w.findingRow(f);
            }

            w.close();

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            document.save(out);
            return out.toByteArray();
        }
    }

    private static final float[] GRAY = {0.4f, 0.45f, 0.42f};
    private static final float[] BLACK = {0.09f, 0.13f, 0.12f};
    private static final float[] ACCENT = {0.12f, 0.48f, 0.39f};
    private static final float[] HIGH = {0.71f, 0.27f, 0.24f};
    private static final float[] MEDIUM = {0.66f, 0.47f, 0.18f};

    /** Cursor-based content-stream writer -- opens a new page automatically when content would run off the bottom margin. */
    private static class Writer implements AutoCloseable {
        private final PDDocument document;
        private final PDFont regular;
        private final PDFont bold;
        private PDPageContentStream cs;
        private float y;

        Writer(PDDocument document) throws IOException {
            this.document = document;
            this.regular = new PDType1Font(Standard14Fonts.FontName.HELVETICA);
            this.bold = new PDType1Font(Standard14Fonts.FontName.HELVETICA_BOLD);
            newPage();
        }

        private void newPage() throws IOException {
            if (cs != null) cs.close();
            PDPage page = new PDPage(PDRectangle.LETTER);
            document.addPage(page);
            cs = new PDPageContentStream(document, page);
            y = PAGE_HEIGHT - MARGIN;
        }

        private void ensureRoom(float needed) throws IOException {
            if (y - needed < MARGIN) newPage();
        }

        void heading(String text, float size) throws IOException {
            gap(size * 0.4f);
            ensureRoom(size + 4);
            draw(text, bold, size, MARGIN, ACCENT);
            y -= size + 6;
        }

        void line(String text, float size) throws IOException {
            line(text, size, false, BLACK);
        }

        void line(String text, float size, boolean boldText) throws IOException {
            line(text, size, boldText, BLACK);
        }

        void line(String text, float size, boolean boldText, float[] color) throws IOException {
            if (text == null || text.isBlank()) return;
            for (String wrapped : wrap(text, boldText ? bold : regular, size, CONTENT_WIDTH)) {
                ensureRoom(size + 4);
                draw(wrapped, boldText ? bold : regular, size, MARGIN, color);
                y -= size + 4;
            }
        }

        void bullet(String text, float size) throws IOException {
            boolean first = true;
            for (String wrapped : wrap(text, regular, size, CONTENT_WIDTH - 14)) {
                ensureRoom(size + 4);
                draw((first ? "-  " : "   ") + wrapped, regular, size, MARGIN, BLACK);
                y -= size + 4;
                first = false;
            }
        }

        void gap(float amount) {
            y -= amount;
        }

        void table(String[] headers, List<String[]> rows, float[] colWidths) throws IOException {
            float rowHeight = 16;
            ensureRoom(rowHeight);
            float x = MARGIN;
            for (int i = 0; i < headers.length; i++) {
                draw(headers[i], bold, 9, x, GRAY);
                x += CONTENT_WIDTH * colWidths[i];
            }
            y -= rowHeight;
            for (String[] row : rows) {
                ensureRoom(rowHeight);
                x = MARGIN;
                for (int i = 0; i < row.length; i++) {
                    draw(row[i], regular, 10, x, BLACK);
                    x += CONTENT_WIDTH * colWidths[i];
                }
                y -= rowHeight;
            }
        }

        void findingRow(ScoreFinding f) throws IOException {
            float[] sevColor = f.points() >= 15 ? HIGH : f.points() >= 5 ? MEDIUM : ACCENT;
            String sevLabel = f.points() >= 15 ? "HIGH" : f.points() >= 5 ? "MEDIUM" : "LOW";

            ensureRoom(28);
            draw("[" + sevLabel + "]", bold, 9, MARGIN, sevColor);
            draw(f.feature() + "  x" + f.count() + "   -- " + f.points() + " pts", bold, 10.5f, MARGIN + 55, BLACK);
            y -= 14;
            for (String wrapped : wrap(f.note(), regular, 9.5f, CONTENT_WIDTH - 55)) {
                ensureRoom(13);
                draw(wrapped, regular, 9.5f, MARGIN + 55, GRAY);
                y -= 13;
            }
            y -= 6;
        }

        private void draw(String text, PDFont font, float size, float x, float[] color) throws IOException {
            cs.beginText();
            cs.setFont(font, size);
            cs.setNonStrokingColor(color[0], color[1], color[2]);
            cs.newLineAtOffset(x, y);
            cs.showText(sanitize(text));
            cs.endText();
        }

        /** PDFBox's built-in Helvetica encoding can't render arbitrary Unicode (curly quotes, em dashes, etc. from source text/notes) -- strip to the safe subset rather than throwing mid-report. */
        private String sanitize(String text) {
            StringBuilder sb = new StringBuilder(text.length());
            for (char c : text.toCharArray()) {
                sb.append(c >= 32 && c < 127 ? c : (c == '\n' || c == '\t' ? ' ' : '?'));
            }
            return sb.toString();
        }

        private List<String> wrap(String text, PDFont font, float size, float maxWidth) throws IOException {
            List<String> lines = new ArrayList<>();
            for (String paragraph : text.split("\n")) {
                StringBuilder current = new StringBuilder();
                for (String word : paragraph.split(" ")) {
                    String candidate = current.isEmpty() ? word : current + " " + word;
                    if (font.getStringWidth(sanitize(candidate)) / 1000 * size > maxWidth && !current.isEmpty()) {
                        lines.add(current.toString());
                        current = new StringBuilder(word);
                    } else {
                        current = new StringBuilder(candidate);
                    }
                }
                if (!current.isEmpty()) lines.add(current.toString());
            }
            return lines;
        }

        @Override
        public void close() throws IOException {
            if (cs != null) cs.close();
        }
    }
}
