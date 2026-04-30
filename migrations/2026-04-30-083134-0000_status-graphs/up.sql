create function timespan_bucket(in_date timestamp, epoch_date timestamp, bucket_size interval) returns integer as $$
    begin
        return ((extract(epoch from in_date - epoch_date)) / extract(epoch from bucket_size))::int;
    end;
$$ language plpgsql;