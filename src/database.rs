use crate::{
    errors::AppError,
    models::{
        CreateFlight, Flight, FlightStatistics, GetScanDataQuery, ScanData, ScanDataInput,
        ScansByHour, TopDevice, UpdateFlight, DecodedBarcode, DecodeRequest, DecodedStatistics,
        CreateRejectionLog, RejectionLog, RejectionLogQuery,
    },
    barcode_parser,
};
use chrono::{DateTime, Local, NaiveDate, Utc};
use sqlx::PgPool;

// Fungsi untuk membuat penerbangan baru di database
pub async fn create_flight(pool: &PgPool, flight: CreateFlight) -> Result<Flight, AppError> {
    // Validasi: departure_time harus sama dengan tanggal scan (scanned_at)
    let scan_date = flight.scanned_at.with_timezone(&Local).date_naive();
    let departure_date = flight.departure_time.with_timezone(&Local).date_naive();

    if departure_date != scan_date {
        tracing::error!(
            scan_date = %scan_date,
            departure_date = %departure_date,
            flight_number = %flight.flight_number,
            "Departure date must match scan date"
        );
        return Err(AppError::InvalidDepartureTime);
    }

    let new_flight = sqlx::query_as!(
        Flight,
        r#"
        INSERT INTO flights (flight_number, airline, aircraft, departure_time, destination, gate, device_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7)
        RETURNING id, flight_number, airline, aircraft, departure_time, destination, gate, is_active, created_at, updated_at, device_id
        "#,
        flight.flight_number,
        flight.airline,
        flight.aircraft,
        flight.departure_time,
        flight.destination,
        flight.gate,
        flight.device_id
    )
        .fetch_one(pool)
        .await;

    // Handle duplicate flight: return existing flight instead of error (idempotent behavior)
    match new_flight {
        Ok(flight) => Ok(flight),
        Err(e) => {
            if let sqlx::Error::Database(db_err) = &e {
                if db_err.constraint() == Some("idx_unique_flight_per_day") {
                    tracing::info!(
                        flight_number = %flight.flight_number,
                        departure_date = %departure_date,
                        "Flight already exists, returning existing flight (idempotent)"
                    );

                    // Get and return existing flight
                    let existing = get_flight_by_number_and_date(
                        pool,
                        &flight.flight_number,
                        departure_date,
                    )
                    .await?
                    .ok_or(AppError::FlightNotFound)?;

                    return Ok(existing);
                }
            }
            Err(AppError::DatabaseError(e))
        }
    }
}

// Fungsi untuk mengambil penerbangan berdasarkan nomor penerbangan dan tanggal (untuk handling duplicate)
pub async fn get_flight_by_number_and_date(
    pool: &PgPool,
    flight_number: &str,
    date: NaiveDate,
) -> Result<Option<Flight>, AppError> {
    let flight = sqlx::query_as!(
        Flight,
        r#"
        SELECT id, flight_number, airline, aircraft, departure_time,
               destination, gate, is_active, created_at, updated_at, device_id
        FROM flights
        WHERE flight_number = $1
          AND (departure_time AT TIME ZONE 'utc')::date = $2
          AND is_active = true
        "#,
        flight_number,
        date
    )
    .fetch_optional(pool)
    .await?;

    Ok(flight)
}

// Fungsi untuk mengambil semua penerbangan, dengan filter tanggal opsional
pub async fn get_all_flights(
    pool: &PgPool,
    date: Option<NaiveDate>,
) -> Result<(Vec<Flight>, i64), AppError> {
    let mut query_builder = sqlx::QueryBuilder::new(
        "SELECT id, flight_number, airline, aircraft, departure_time, destination, gate, is_active, created_at, updated_at, device_id FROM flights WHERE is_active = true ",
    );
    let mut count_builder =
        sqlx::QueryBuilder::new("SELECT COUNT(*) FROM flights WHERE is_active = true ");

    if let Some(d) = date {
        // Casting ke date harus dilakukan dengan zona waktu yang benar
        query_builder.push("AND (departure_time AT TIME ZONE 'utc')::date = ");
        query_builder.push_bind(d);
        count_builder.push("AND (departure_time AT TIME ZONE 'utc')::date = ");
        count_builder.push_bind(d);
    }

    query_builder.push(" ORDER BY departure_time ASC");

    let flights = query_builder.build_query_as::<Flight>().fetch_all(pool).await?;
    let total: (i64,) = count_builder.build_query_as().fetch_one(pool).await?;

    Ok((flights, total.0))
}


// Fungsi untuk mengambil satu penerbangan berdasarkan ID
pub async fn get_flight_by_id(pool: &PgPool, id: i32) -> Result<Flight, AppError> {
    let flight = sqlx::query_as!(
        Flight,
        "SELECT id, flight_number, airline, aircraft, departure_time, destination, gate, is_active, created_at, updated_at, device_id FROM flights WHERE id = $1 AND is_active = true",
        id
    )
        .fetch_optional(pool)
        .await?
        .ok_or(AppError::FlightNotFound)?;

    Ok(flight)
}

// Fungsi untuk memperbarui data penerbangan
pub async fn update_flight(
    pool: &PgPool,
    id: i32,
    flight: UpdateFlight,
) -> Result<Flight, AppError> {
    let updated_flight = sqlx::query_as!(
        Flight,
        r#"
        UPDATE flights
        SET
            airline = COALESCE($1, airline),
            aircraft = COALESCE($2, aircraft),
            departure_time = COALESCE($3, departure_time),
            destination = COALESCE($4, destination),
            gate = COALESCE($5, gate),
            is_active = COALESCE($6, is_active),
            updated_at = NOW()
        WHERE id = $7
        RETURNING id, flight_number, airline, aircraft, departure_time, destination, gate, is_active, created_at, updated_at, device_id
        "#,
        flight.airline,
        flight.aircraft,
        flight.departure_time,
        flight.destination,
        flight.gate,
        flight.is_active,
        id
    )
        .fetch_one(pool)
        .await?;

    Ok(updated_flight)
}

// Fungsi untuk soft delete penerbangan
pub async fn delete_flight(pool: &PgPool, id: i32) -> Result<(), AppError> {
    let result = sqlx::query!(
        "UPDATE flights SET is_active = false, updated_at = NOW() WHERE id = $1",
        id
    )
        .execute(pool)
        .await?;

    if result.rows_affected() == 0 {
        return Err(AppError::FlightNotFound);
    }

    Ok(())
}

// Fungsi untuk mengambil statistik penerbangan
pub async fn get_flight_statistics(pool: &PgPool, id: i32) -> Result<FlightStatistics, AppError> {
    let flight_info = get_flight_by_id(pool, id).await?;

    let total_scans: (i64,) =
        sqlx::query_as("SELECT COUNT(*) FROM scan_data WHERE flight_id = $1")
            .bind(id)
            .fetch_one(pool)
            .await?;

    let unique_scans: (i64,) = sqlx::query_as(
        "SELECT COUNT(DISTINCT barcode_value) FROM scan_data WHERE flight_id = $1",
    )
        .bind(id)
        .fetch_one(pool)
        .await?;

    let scans_by_hour: Vec<ScansByHour> = sqlx::query_as(
        r#"
        SELECT TO_CHAR(DATE_TRUNC('hour', scan_time), 'HH24:00') as hour, COUNT(*) as count
        FROM scan_data
        WHERE flight_id = $1
        GROUP BY DATE_TRUNC('hour', scan_time)
        ORDER BY hour
        "#,
    )
        .bind(id)
        .fetch_all(pool)
        .await?;

    let top_devices: Vec<TopDevice> = sqlx::query_as(
        r#"
        SELECT device_id, COUNT(*) as scan_count
        FROM scan_data
        WHERE flight_id = $1
        GROUP BY device_id
        ORDER BY scan_count DESC
        LIMIT 5
        "#,
    )
        .bind(id)
        .fetch_all(pool)
        .await?;

    Ok(FlightStatistics {
        flight_id: id,
        flight_number: flight_info.flight_number,
        total_scans: total_scans.0,
        unique_scans: unique_scans.0,
        duplicate_scans: total_scans.0 - unique_scans.0,
        scans_by_hour,
        top_devices,
    })
}

// Fungsi untuk mengambil statistik decoded barcodes per penerbangan
pub async fn get_decoded_statistics(
    pool: &PgPool,
    flight_id: i32,
) -> Result<DecodedStatistics, AppError> {
    // Get flight info first
    let flight = get_flight_by_id(pool, flight_id).await?;

    // Count total decoded barcodes (JOIN with scan_data by flight_id)
    let total_decoded: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*)
        FROM decode_barcode db
        JOIN scan_data sd ON db.scan_data_id = sd.id
        WHERE sd.flight_id = $1
        "#,
    )
    .bind(flight_id)
    .fetch_one(pool)
    .await?;

    // Count infant passengers
    let infant_count: (i64,) = sqlx::query_as(
        r#"
        SELECT COUNT(*)
        FROM decode_barcode db
        JOIN scan_data sd ON db.scan_data_id = sd.id
        WHERE sd.flight_id = $1 AND db.infant_status = true
        "#,
    )
    .bind(flight_id)
    .fetch_one(pool)
    .await?;

    Ok(DecodedStatistics {
        flight_id,
        flight_number: flight.flight_number,
        total_decoded: total_decoded.0,
        infant_count: infant_count.0,
        adult_count: total_decoded.0 - infant_count.0,
    })
}

// Fungsi untuk membuat data scan baru
pub async fn create_scan_data(
    pool: &PgPool,
    scan: ScanDataInput,
) -> Result<ScanData, AppError> {
    // Pastikan flight_id valid
    let _ = get_flight_by_id(pool, scan.flight_id).await?;

    // Check for duplicate scan (same barcode + same flight)
    let existing_scan = sqlx::query_as!(
        ScanData,
        r#"
        SELECT id, barcode_value, barcode_format, scan_time, device_id, flight_id, created_at
        FROM scan_data
        WHERE barcode_value = $1 AND flight_id = $2
        LIMIT 1
        "#,
        scan.barcode_value,
        scan.flight_id,
    )
    .fetch_optional(pool)
    .await?;

    // If duplicate found, return HTTP 409 Conflict
    if let Some(existing) = existing_scan {
        return Err(AppError::DuplicateScan {
            barcode: scan.barcode_value.clone(),
            flight_id: scan.flight_id,
            existing_scan_id: existing.id,
        });
    }

    // No duplicate, proceed with insert
    let new_scan = sqlx::query_as!(
        ScanData,
        r#"
        INSERT INTO scan_data (barcode_value, barcode_format, device_id, flight_id)
        VALUES ($1, $2, $3, $4)
        RETURNING id, barcode_value, barcode_format, scan_time, device_id, flight_id, created_at
        "#,
        scan.barcode_value,
        scan.barcode_format,
        scan.device_id,
        scan.flight_id,
    )
        .fetch_one(pool)
        .await;

    // Handle constraint violation (race condition: two concurrent requests)
    match new_scan {
        Ok(scan_data) => Ok(scan_data),
        Err(e) => {
            if let sqlx::Error::Database(db_err) = &e {
                if db_err.constraint() == Some("idx_unique_barcode_per_flight") {
                    // Race condition occurred - another request inserted first
                    // Fetch the existing scan to return proper 409 response
                    let existing = sqlx::query_as!(
                        ScanData,
                        r#"
                        SELECT id, barcode_value, barcode_format, scan_time, device_id, flight_id, created_at
                        FROM scan_data
                        WHERE barcode_value = $1 AND flight_id = $2
                        LIMIT 1
                        "#,
                        scan.barcode_value,
                        scan.flight_id,
                    )
                    .fetch_one(pool)
                    .await?;

                    return Err(AppError::DuplicateScan {
                        barcode: scan.barcode_value,
                        flight_id: scan.flight_id,
                        existing_scan_id: existing.id,
                    });
                }
            }
            Err(AppError::DatabaseError(e))
        }
    }
}

// Fungsi untuk mengambil data scan dengan filter
pub async fn get_scan_data(
    pool: &PgPool,
    query: GetScanDataQuery,
) -> Result<(Vec<ScanData>, i64), AppError> {
    let mut query_builder = sqlx::QueryBuilder::new("SELECT id, barcode_value, barcode_format, scan_time, device_id, flight_id, created_at FROM scan_data WHERE 1=1 ");
    let mut count_builder = sqlx::QueryBuilder::new("SELECT COUNT(*) FROM scan_data WHERE 1=1 ");

    if let Some(flight_id) = query.flight_id {
        query_builder.push(" AND flight_id = ").push_bind(flight_id);
        count_builder.push(" AND flight_id = ").push_bind(flight_id);
    }

    if let Some(date_range) = query.date_range {
        let parts: Vec<&str> = date_range.split(',').collect();
        if parts.len() == 2
            && let (Ok(start), Ok(end)) = (parts[0].parse::<DateTime<Utc>>(), parts[1].parse::<DateTime<Utc>>())
        {
            query_builder.push(" AND scan_time BETWEEN ").push_bind(start).push(" AND ").push_bind(end);
            count_builder.push(" AND scan_time BETWEEN ").push_bind(start).push(" AND ").push_bind(end);
        }
    }

    let scans = query_builder.build_query_as::<ScanData>().fetch_all(pool).await?;
    let total: (i64,) = count_builder.build_query_as().fetch_one(pool).await?;

    Ok((scans, total.0))
}


// Fungsi untuk mengambil penerbangan sejak timestamp terakhir
pub async fn get_flights_since(
    pool: &PgPool,
    last_sync: Option<DateTime<Utc>>,
) -> Result<Vec<Flight>, AppError> {
    let flights = match last_sync {
        Some(ts) => {
            sqlx::query_as!(Flight, "SELECT id, flight_number, airline, aircraft, departure_time, destination, gate, is_active, created_at, updated_at, device_id FROM flights WHERE updated_at > $1 OR created_at > $1 ORDER BY updated_at", ts)
                .fetch_all(pool)
                .await?
        }
        None => {
            sqlx::query_as!(Flight, "SELECT id, flight_number, airline, aircraft, departure_time, destination, gate, is_active, created_at, updated_at, device_id FROM flights ORDER BY created_at")
                .fetch_all(pool)
                .await?
        }
    };
    Ok(flights)
}

// Fungsi untuk bulk insert flights (TELAH DIPERBAIKI)
pub async fn bulk_insert_flights(
    pool: &PgPool,
    flights: Vec<CreateFlight>,
) -> Result<usize, AppError> {
    let mut tx = pool.begin().await?;
    let mut total_affected: u64 = 0;

    for flight in flights {
        if flight.departure_time < Utc::now() {
            // Kita bisa skip atau return error, di sini kita skip
            continue;
        }

        let result = sqlx::query(
            r#"
            INSERT INTO flights (flight_number, airline, aircraft, departure_time, destination, gate, device_id)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (flight_number, ((departure_time AT TIME ZONE 'utc')::date)) DO UPDATE SET
                airline = EXCLUDED.airline,
                aircraft = EXCLUDED.aircraft,
                departure_time = EXCLUDED.departure_time,
                destination = EXCLUDED.destination,
                gate = EXCLUDED.gate,
                updated_at = NOW()
            "#
        )
            .bind(&flight.flight_number)
            .bind(&flight.airline)
            .bind(&flight.aircraft)
            .bind(flight.departure_time)
            .bind(&flight.destination)
            .bind(&flight.gate)
            .bind(&flight.device_id)
            .execute(&mut *tx)
            .await?;

        total_affected += result.rows_affected();
    }

    tx.commit().await?;

    Ok(total_affected as usize)
}

// Barcode decoder functions

// Fungsi untuk decode barcode IATA format
// Uses shared parser module synchronized with mobile app
pub async fn decode_barcode_iata(
    pool: &PgPool,
    request: DecodeRequest,
) -> Result<DecodedBarcode, AppError> {
    // Use shared parser (synchronized with mobile app)
    let parsed = barcode_parser::parse_iata_bcbp(&request.barcode_value)
        .ok_or(AppError::InvalidBarcodeFormat)?;

    // Extract data from parsed result
    let passenger_name = parsed.passenger_name;
    let booking_code = parsed.booking_code;
    let origin = parsed.origin;
    let destination = parsed.destination;
    let airline_code = parsed.airline_code;
    let flight_number = parsed.flight_number.parse::<i32>().unwrap_or(0);
    let flight_date_julian = parsed.flight_date_julian;
    let cabin_class = parsed.cabin_class;
    let seat_number = parsed.seat_number;
    let sequence_number = parsed.sequence_number;
    let infant_status = parsed.infant_status;

    let decoded = sqlx::query_as!(
        DecodedBarcode,
        r#"
        INSERT INTO decode_barcode
        (barcode_value, passenger_name, booking_code, origin, destination, airline_code,
         flight_number, flight_date_julian, cabin_class, seat_number, sequence_number,
         infant_status, scan_data_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        RETURNING id, barcode_value, passenger_name, booking_code, origin, destination,
                  airline_code, flight_number, flight_date_julian, cabin_class, seat_number,
                  sequence_number, infant_status, scan_data_id, created_at
        "#,
        request.barcode_value,
        passenger_name,
        booking_code,
        origin,
        destination,
        airline_code,
        flight_number,
        flight_date_julian,
        cabin_class,
        seat_number,
        sequence_number,
        infant_status,
        request.scan_data_id
    )
    .fetch_one(pool)
    .await?;

    Ok(decoded)
}

// Fungsi untuk mengambil semua decoded barcodes dengan filter flight_id optional
pub async fn get_all_decoded_barcodes(
    pool: &PgPool,
    flight_id: Option<i32>,
) -> Result<Vec<DecodedBarcode>, AppError> {
    let decoded_list = if let Some(fid) = flight_id {
        // Filter by flight_id via JOIN dengan scan_data
        sqlx::query_as!(
            DecodedBarcode,
            r#"
            SELECT db.id, db.barcode_value, db.passenger_name, db.booking_code, db.origin, db.destination,
                   db.airline_code, db.flight_number, db.flight_date_julian, db.cabin_class, db.seat_number,
                   db.sequence_number, db.infant_status, db.scan_data_id, db.created_at
            FROM decode_barcode db
            JOIN scan_data sd ON db.scan_data_id = sd.id
            WHERE sd.flight_id = $1
            ORDER BY db.created_at DESC
            "#,
            fid
        )
        .fetch_all(pool)
        .await?
    } else {
        // Get all decoded barcodes
        sqlx::query_as!(
            DecodedBarcode,
            r#"
            SELECT id, barcode_value, passenger_name, booking_code, origin, destination,
                   airline_code, flight_number, flight_date_julian, cabin_class, seat_number,
                   sequence_number, infant_status, scan_data_id, created_at
            FROM decode_barcode
            ORDER BY created_at DESC
            "#
        )
        .fetch_all(pool)
        .await?
    };

    Ok(decoded_list)
}

// NOTE: All parsing logic has been moved to shared barcode_parser module
// This ensures 100% synchronization between mobile app and server

// ==================== REJECTION LOGGING FUNCTIONS ====================

/// Create a rejection log entry in server database
pub async fn create_rejection_log(
    pool: &PgPool,
    log: CreateRejectionLog,
) -> Result<RejectionLog, AppError> {
    let rejection = sqlx::query_as!(
        RejectionLog,
        r#"
        INSERT INTO rejection_logs
        (barcode_value, barcode_format, reason, expected_date, actual_date,
         flight_number, airline, device_id)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        RETURNING id, barcode_value, barcode_format, reason, expected_date, actual_date,
                  flight_number, airline, device_id, rejected_at
        "#,
        log.barcode_value,
        log.barcode_format,
        log.reason,
        log.expected_date,
        log.actual_date,
        log.flight_number,
        log.airline,
        log.device_id
    )
    .fetch_one(pool)
    .await?;

    Ok(rejection)
}

/// Get rejection logs with optional filtering
pub async fn get_rejection_logs(
    pool: &PgPool,
    query: RejectionLogQuery,
) -> Result<Vec<RejectionLog>, AppError> {
    let limit = query.limit.unwrap_or(100);
    let offset = query.offset.unwrap_or(0);

    let mut query_builder = String::from(
        "SELECT id, barcode_value, barcode_format, reason, expected_date, actual_date,
                flight_number, airline, device_id, rejected_at
         FROM rejection_logs
         WHERE 1=1"
    );

    // Add filters
    if query.airline.is_some() {
        query_builder.push_str(" AND airline = $1");
    }
    if query.reason.is_some() {
        query_builder.push_str(" AND reason LIKE $2");
    }
    if query.device_id.is_some() {
        query_builder.push_str(" AND device_id = $3");
    }

    query_builder.push_str(" ORDER BY rejected_at DESC LIMIT $4 OFFSET $5");

    // Execute query with parameters
    let logs = if let (Some(airline), Some(reason), Some(device_id)) =
        (&query.airline, &query.reason, &query.device_id) {
        sqlx::query_as::<_, RejectionLog>(&query_builder)
            .bind(airline)
            .bind(format!("%{}%", reason))
            .bind(device_id)
            .bind(limit)
            .bind(offset)
            .fetch_all(pool)
            .await?
    } else if let (Some(airline), Some(reason)) = (&query.airline, &query.reason) {
        sqlx::query_as::<_, RejectionLog>(&query_builder.replace("$3", ""))
            .bind(airline)
            .bind(format!("%{}%", reason))
            .bind(limit)
            .bind(offset)
            .fetch_all(pool)
            .await?
    } else if let Some(airline) = &query.airline {
        sqlx::query_as::<_, RejectionLog>(&query_builder.replace("$2", "").replace("$3", ""))
            .bind(airline)
            .bind(limit)
            .bind(offset)
            .fetch_all(pool)
            .await?
    } else {
        sqlx::query_as::<_, RejectionLog>(
            "SELECT id, barcode_value, barcode_format, reason, expected_date, actual_date,
                    flight_number, airline, device_id, rejected_at
             FROM rejection_logs
             ORDER BY rejected_at DESC
             LIMIT $1 OFFSET $2"
        )
        .bind(limit)
        .bind(offset)
        .fetch_all(pool)
        .await?
    };

    Ok(logs)
}

/// Get rejection statistics
pub async fn get_rejection_stats(
    pool: &PgPool,
) -> Result<serde_json::Value, AppError> {
    let stats = sqlx::query!(
        r#"
        SELECT
            COUNT(*) as total_rejections,
            COUNT(DISTINCT airline) as airlines_count,
            COUNT(DISTINCT device_id) as devices_count,
            COUNT(CASE WHEN reason LIKE '%date_mismatch%' THEN 1 END) as date_mismatch_count,
            COUNT(CASE WHEN reason LIKE '%invalid_format%' THEN 1 END) as invalid_format_count
        FROM rejection_logs
        WHERE rejected_at >= NOW() - INTERVAL '30 days'
        "#
    )
    .fetch_one(pool)
    .await?;

    Ok(serde_json::json!({
        "totalRejections": stats.total_rejections.unwrap_or(0),
        "airlinesCount": stats.airlines_count.unwrap_or(0),
        "devicesCount": stats.devices_count.unwrap_or(0),
        "dateMismatchCount": stats.date_mismatch_count.unwrap_or(0),
        "invalidFormatCount": stats.invalid_format_count.unwrap_or(0),
    }))
}

// ============= Translation/Code Mapping Database Functions =============

/// Get all airport codes
pub async fn get_airport_codes(
    pool: &PgPool,
) -> Result<Vec<crate::models::AirportCode>, AppError> {
    let codes = sqlx::query_as!(
        crate::models::AirportCode,
        r#"
        SELECT id, code, name, city, country,
               created_at as "created_at!",
               updated_at as "updated_at!"
        FROM airport_codes
        ORDER BY code
        "#
    )
    .fetch_all(pool)
    .await?;

    Ok(codes)
}

/// Get all airline codes
pub async fn get_airline_codes(
    pool: &PgPool,
) -> Result<Vec<crate::models::AirlineCode>, AppError> {
    let codes = sqlx::query_as!(
        crate::models::AirlineCode,
        r#"
        SELECT id, code, name, country,
               created_at as "created_at!",
               updated_at as "updated_at!"
        FROM airline_codes
        ORDER BY code
        "#
    )
    .fetch_all(pool)
    .await?;

    Ok(codes)
}

/// Get all cabin class codes
pub async fn get_cabin_class_codes(
    pool: &PgPool,
) -> Result<Vec<crate::models::CabinClassCode>, AppError> {
    let codes = sqlx::query_as!(
        crate::models::CabinClassCode,
        r#"
        SELECT id, code, name, description,
               created_at as "created_at!",
               updated_at as "updated_at!"
        FROM cabin_class_codes
        ORDER BY code
        "#
    )
    .fetch_all(pool)
    .await?;

    Ok(codes)
}

/// Get starter data version
pub async fn get_starter_data_version(
    pool: &PgPool,
) -> Result<crate::models::StarterDataVersion, AppError> {
    let version = sqlx::query_as!(
        crate::models::StarterDataVersion,
        r#"
        SELECT id, version,
               updated_at as "updated_at!"
        FROM starter_data_version
        ORDER BY id DESC
        LIMIT 1
        "#
    )
    .fetch_one(pool)
    .await?;

    Ok(version)
}
