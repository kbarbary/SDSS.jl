# Adapted from the Python astroquery.sdss module, which is released under
# a 3-clause BSD style license.

using HTTPClient: get
import JSON

const TIMEOUT = 60.0

"Base URL for catalog-related queries like SQL and Cross-ID."
const SKYSERVER_BASEURL = "http://skyserver.sdss.org"

"Base URL for downloading data products like spectra and images."
const SAS_BASEURL = "http://data.sdss3.org/sas"


function get_query_url(data_release)
    suffix = (data_release < 11)? "sql.asp": "x_sql.aspx"
    return string(SKYSERVER_BASEURL, "/dr$(data_release)/en/tools/search/",
                  suffix)
end


imaging_url_suffix(base, dr, rerun, run, camcol, band, field) =
    @sprintf("%s/dr%d/boss/photoObj/frames/%d/%d/%d/frame-%s-%06d-%d-%04d.fits.bz2",
             base, dr, rerun, run, camcol, band, run, camcol, field)


"""Remove comments and newlines from SQL statement."""
function sanitize_query(stmt::AbstractString)
    lines = split(stmt, '\n')
    for i in 1:length(lines)
        r = search(lines[i], "--")
        if r != 0:-1
            lines[i] = lines[i][1:(first(r)-1)]
        end
    end
    return join(lines, ' ')
end


"""
query_sdss_sql(sql_query; data_release=12, timeout=60.0)

Query the SDSS database. Returns an IOBuffer with the contents.
Use `bytestring()` to convert this to a string if desired.
"""
function query_sdss_sql(sql_query::AbstractString;
                        data_release=12, timeout=TIMEOUT)

    url = get_query_url(data_release)
    payload = [("cmd", sanitize_query(sql_query)), ("format", "csv")]
    res = get(url; query_params=payload, request_timeout=timeout)
    if res.http_code != 200
        error("request for $url returned status $(res.http_code)")
    end

    return res.body
end

