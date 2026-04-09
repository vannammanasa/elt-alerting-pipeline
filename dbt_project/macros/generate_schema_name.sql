-- =============================================================================
-- generate_schema_name.sql — Custom Schema Macro
-- =============================================================================
-- Overrides dbt's default schema naming behaviour.  By default, dbt
-- prepends the target schema (e.g. "transformed_staging") which breaks
-- the Deliverable 1 schema layout.  This macro uses the custom schema
-- directly when one is specified, falling back to the target schema.
-- =============================================================================

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
