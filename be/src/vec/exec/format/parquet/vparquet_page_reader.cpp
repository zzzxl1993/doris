// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "vparquet_page_reader.h"

#include <gen_cpp/parquet_types.h>
#include <stddef.h>
#include <stdint.h>

#include <algorithm>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "io/fs/buffered_reader.h"
#include "util/runtime_profile.h"
#include "util/slice.h"
#include "util/thrift_util.h"

namespace doris {
namespace io {
struct IOContext;
} // namespace io
} // namespace doris

namespace doris::vectorized {
#include "common/compile_check_begin.h"
static constexpr size_t INIT_PAGE_HEADER_SIZE = 128;

std::unique_ptr<PageReader> create_page_reader(io::BufferedStreamReader* reader,
                                               io::IOContext* io_ctx, uint64_t offset,
                                               uint64_t length, int64_t num_values,
                                               const tparquet::OffsetIndex* offset_index) {
    if (offset_index) {
        return std::make_unique<PageReaderWithOffsetIndex>(reader, io_ctx, offset, length,
                                                           num_values, offset_index);
    } else {
        return std::make_unique<PageReader>(reader, io_ctx, offset, length);
    }
}

PageReader::PageReader(io::BufferedStreamReader* reader, io::IOContext* io_ctx, uint64_t offset,
                       uint64_t length)
        : _reader(reader), _io_ctx(io_ctx), _start_offset(offset), _end_offset(offset + length) {}

Status PageReader::_parse_page_header() {
    if (UNLIKELY(_offset < _start_offset || _offset >= _end_offset)) {
        return Status::IOError("Out-of-bounds Access");
    }
    if (UNLIKELY(_offset != _next_header_offset)) {
        return Status::IOError("Wrong header position, should seek to a page header first");
    }
    if (UNLIKELY(_state != INITIALIZED)) {
        return Status::IOError("Should skip or load current page to get next page");
    }

    const uint8_t* page_header_buf = nullptr;
    size_t max_size = _end_offset - _offset;
    size_t header_size = std::min(INIT_PAGE_HEADER_SIZE, max_size);
    const size_t MAX_PAGE_HEADER_SIZE = config::parquet_header_max_size_mb << 20;
    uint32_t real_header_size = 0;
    while (true) {
        if (UNLIKELY(_io_ctx && _io_ctx->should_stop)) {
            return Status::EndOfFile("stop");
        }
        header_size = std::min(header_size, max_size);
        RETURN_IF_ERROR(_reader->read_bytes(&page_header_buf, _offset, header_size, _io_ctx));
        real_header_size = cast_set<uint32_t>(header_size);
        SCOPED_RAW_TIMER(&_statistics.decode_header_time);
        auto st =
                deserialize_thrift_msg(page_header_buf, &real_header_size, true, &_cur_page_header);
        if (st.ok()) {
            break;
        }
        if (_offset + header_size >= _end_offset || real_header_size > MAX_PAGE_HEADER_SIZE) {
            return Status::IOError(
                    "Failed to deserialize parquet page header. offset: {}, "
                    "header size: {}, end offset: {}, real header size: {}",
                    _offset, header_size, _end_offset, real_header_size);
        }
        header_size <<= 2;
    }

    _statistics.parse_page_header_num++;
    _offset += real_header_size;
    _next_header_offset = _offset + _cur_page_header.compressed_page_size;
    _state = HEADER_PARSED;
    return Status::OK();
}

Status PageReader::skip_page() {
    if (UNLIKELY(_state != HEADER_PARSED)) {
        return Status::IOError("Should generate page header first to skip current page");
    }
    _offset = _next_header_offset;
    _state = INITIALIZED;
    return Status::OK();
}

Status PageReader::get_page_data(Slice& slice) {
    if (UNLIKELY(_state != HEADER_PARSED)) {
        return Status::IOError("Should generate page header first to load current page data");
    }
    if (UNLIKELY(_io_ctx && _io_ctx->should_stop)) {
        return Status::EndOfFile("stop");
    }
    slice.size = _cur_page_header.compressed_page_size;
    RETURN_IF_ERROR(_reader->read_bytes(slice, _offset, _io_ctx));
    _offset += slice.size;
    _state = INITIALIZED;
    return Status::OK();
}
#include "common/compile_check_end.h"

} // namespace doris::vectorized
