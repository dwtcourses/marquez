ALTER TABLE jobs ADD job_context_template_uuid UUID;
ALTER TABLE jobs ADD location_template varchar;
ALTER TABLE jobs ADD inputs_template JSONB;
ALTER TABLE jobs ADD outputs_template JSONB;
