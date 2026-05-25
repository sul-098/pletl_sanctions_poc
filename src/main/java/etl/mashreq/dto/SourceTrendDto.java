package etl.mashreq.dto;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.time.LocalDate;

@Data
@AllArgsConstructor
public class SourceTrendDto {
    private LocalDate week;
    private String sourceCode;
    private Integer totalAdded;
    private Integer totalRemoved;
    private Integer totalUpdated;
}
