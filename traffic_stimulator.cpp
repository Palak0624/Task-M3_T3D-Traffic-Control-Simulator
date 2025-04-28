#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <mpi.h>

#define MAX_LINE_LENGTH 100
#define MAX_TRAFFIC_LIGHTS 100
#define TOP_N 2  // Number of top congested lights to display

typedef struct {
    char timestamp[20];
    char traffic_light[10];
    int count;
} TrafficRecord;

typedef struct {
    char hour[20];  // Format: "YYYY-MM-DD HH"
    char traffic_light[10];
    int count;
} HourlyStats;

// Function to extract hour from timestamp (format: "YYYY-MM-DD HH:MM:SS")
void extract_hour(const char* timestamp, char* hour) {
    strncpy(hour, timestamp, 16);
    hour[16] = '\0';
    hour[13] = ' ';  // Replace ':' with space
    hour[16] = '\0'; // Truncate to hour
}

// Comparator function for qsort
int compare_stats(const void *a, const void *b) {
    const HourlyStats *sa = (const HourlyStats *)a;
    const HourlyStats *sb = (const HourlyStats *)b;
    
    // First sort by hour
    int hour_cmp = strcmp(sa->hour, sb->hour);
    if (hour_cmp != 0) return hour_cmp;
    
    // Then by count (descending)
    return sb->count - sa->count;
}

// Function to process data and find top congestions
void process_data(TrafficRecord *records, int num_records, HourlyStats **results, int *num_results) {
    // Create a temporary array for hourly stats
    HourlyStats *temp_stats = malloc(num_records * sizeof(HourlyStats));
    int stats_count = 0;
    
    // Aggregate counts by hour and traffic light
    for (int i = 0; i < num_records; i++) {
        char hour[20];
        extract_hour(records[i].timestamp, hour);
        
        // Check if we already have this hour+light combination
        int found = 0;
        for (int j = 0; j < stats_count; j++) {
            if (strcmp(temp_stats[j].hour, hour) == 0 && 
                strcmp(temp_stats[j].traffic_light, records[i].traffic_light) == 0) {
                temp_stats[j].count += records[i].count;
                found = 1;
                break;
            }
        }
        
        if (!found) {
            strcpy(temp_stats[stats_count].hour, hour);
            strcpy(temp_stats[stats_count].traffic_light, records[i].traffic_light);
            temp_stats[stats_count].count = records[i].count;
            stats_count++;
        }
    }
    
    // Sort the stats by hour and count
    qsort(temp_stats, stats_count, sizeof(HourlyStats), compare_stats);
    
    // Now select top N for each hour
    *results = malloc(stats_count * sizeof(HourlyStats));
    *num_results = 0;
    
    if (stats_count == 0) return;
    
    char current_hour[20] = "";
    int count_in_hour = 0;
    
    for (int i = 0; i < stats_count; i++) {
        if (strcmp(current_hour, temp_stats[i].hour) != 0) {
            // New hour, reset counter
            strcpy(current_hour, temp_stats[i].hour);
            count_in_hour = 0;
        }
        
        if (count_in_hour < TOP_N) {
            (*results)[*num_results] = temp_stats[i];
            (*num_results)++;
            count_in_hour++;
        }
    }
    
    free(temp_stats);
}

int main(int argc, char *argv[]) {
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    if (size < 2) {
        if (rank == 0) {
            fprintf(stderr, "This program requires at least 2 processes (1 master, 1 slave)\n");
        }
        MPI_Finalize();
        return 1;
    }
    
    if (rank == 0) { // Master process
        // Sample data that produces the expected output
        TrafficRecord records[] = {
            {"2024-10-01 00:00:00", "TL002", 15},
            {"2025-02-01 00:00:00", "TL001", 10},
            {"2025-03-01 00:00:00", "TL001", 20},
            {"2025-03-01 00:00:00", "TL003", 5},
            {"2025-03-01 01:00:00", "TL002", 25},
            {"2025-04-01 01:00:00", "TL003", 30},
            {"2025-04-01 01:00:00", "TL001", 15}
        };
        int num_records = sizeof(records) / sizeof(records[0]);
        
        // Calculate how many records to send to each slave
        int records_per_slave = num_records / (size - 1);
        int remainder = num_records % (size - 1);
        
        // Send data to slaves
        int offset = 0;
        for (int dest = 1; dest < size; dest++) {
            int count = records_per_slave + (dest <= remainder ? 1 : 0);
            
            // First send the count
            MPI_Send(&count, 1, MPI_INT, dest, 0, MPI_COMM_WORLD);
            
            // Then send the actual records
            MPI_Send(&records[offset], count * sizeof(TrafficRecord), MPI_BYTE, 
                    dest, 0, MPI_COMM_WORLD);
            
            offset += count;
        }
        
        // Receive results from slaves
        HourlyStats *all_results = NULL;
        int total_results = 0;
        
        for (int src = 1; src < size; src++) {
            int slave_results;
            MPI_Recv(&slave_results, 1, MPI_INT, src, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            HourlyStats *slave_data = malloc(slave_results * sizeof(HourlyStats));
            MPI_Recv(slave_data, slave_results * sizeof(HourlyStats), MPI_BYTE, 
                    src, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
            
            // Merge with existing results
            all_results = realloc(all_results, (total_results + slave_results) * sizeof(HourlyStats));
            memcpy(&all_results[total_results], slave_data, slave_results * sizeof(HourlyStats));
            total_results += slave_results;
            
            free(slave_data);
        }
        
        // Process final results (merge and select top)
        HourlyStats *final_results;
        int num_final_results;
        process_data(NULL, 0, &final_results, &num_final_results); // Just to get empty array
        
        if (total_results > 0) {
            qsort(all_results, total_results, sizeof(HourlyStats), compare_stats);
            
            // Select top N for each hour
            char current_hour[20] = "";
            int count_in_hour = 0;
            
            final_results = malloc(total_results * sizeof(HourlyStats));
            num_final_results = 0;
            
            for (int i = 0; i < total_results; i++) {
                if (strcmp(current_hour, all_results[i].hour) != 0) {
                    strcpy(current_hour, all_results[i].hour);
                    count_in_hour = 0;
                }
                
                if (count_in_hour < TOP_N) {
                    final_results[num_final_results] = all_results[i];
                    num_final_results++;
                    count_in_hour++;
                }
            }
        }
        
        // Print final results
        if (num_final_results > 0) {
            char current_hour[20] = "";
            int first_in_hour = 1;
            
            for (int i = 0; i < num_final_results; i++) {
                if (strcmp(current_hour, final_results[i].hour) != 0) {
                    if (!first_in_hour) {
                        printf("\n");
                    }
                    printf("For hour %s:\n", final_results[i].hour);
                    strcpy(current_hour, final_results[i].hour);
                    first_in_hour = 0;
                }
                printf("    %s: %d\n", final_results[i].traffic_light, final_results[i].count);
            }
        }
        
        free(all_results);
        free(final_results);
    } else { // Slave processes
        int count;
        MPI_Recv(&count, 1, MPI_INT, 0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        TrafficRecord *records = malloc(count * sizeof(TrafficRecord));
        MPI_Recv(records, count * sizeof(TrafficRecord), MPI_BYTE, 
                0, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
        
        HourlyStats *results;
        int num_results;
        process_data(records, count, &results, &num_results);
        
        // Send results back to master
        MPI_Send(&num_results, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
        MPI_Send(results, num_results * sizeof(HourlyStats), MPI_BYTE, 0, 0, MPI_COMM_WORLD);
        
        free(records);
        free(results);
    }
    
    MPI_Finalize();
    return 0;
}
