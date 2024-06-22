#pragma once

#include "duckdb.h"
#include "duckdb_node_addon.hpp"
#include "napi.h"

namespace duckdb_node {

// C++ 20 magic ^^
template <size_t N>
struct StringLiteral {
	constexpr StringLiteral(const char (&str)[N]) {
		std::copy_n(str, N, value);
	}
	char value[N];
};

typedef uint8_t data_t;

static Napi::Value GetValue(const Napi::CallbackInfo &info, size_t offset) {
	Napi::Env env = info.Env();

	if (info.Length() < offset) {
		throw Napi::TypeError::New(env, "Value expected at offset " + std::to_string(offset));
	}
	return info[offset].As<Napi::Value>();
}

template <class T>
class PointerHolder : public Napi::ObjectWrap<PointerHolder<T>> {
public:
	static void Init(Napi::Env env, Napi::Object exports,
	                 std::unordered_map<const char *, Napi::FunctionReference> &constructors, const char *name) {
		auto func = Napi::ObjectWrap<PointerHolder<T>>::DefineClass(env, name, {});
		constructor_key = name;
		constructors[constructor_key] = Napi::Persistent(func); // set initial reference count to 1
		exports.Set(name, func);
	}

	PointerHolder(const Napi::CallbackInfo &info) : Napi::ObjectWrap<PointerHolder<T>>(info) {
		ptr = std::unique_ptr<data_t[]>(new data_t[sizeof(T)]);
	}

	static T *FromInfo(const Napi::CallbackInfo &info, idx_t offset) {
		return Napi::ObjectWrap<PointerHolder<T>>::Unwrap(GetValue(info, offset).As<Napi::Object>())->Get();
	}

	static Napi::Value NewAndSet(Napi::Env &env, T val) {
		auto addon = env.GetInstanceData<DuckDBNodeAddon>();
		auto res = addon->constructors[constructor_key].New({});
		Napi::ObjectWrap<PointerHolder<T>>::Unwrap(res)->Set(val);
		return res;
	}

	T *Get() {
		return (T *)ptr.get();
	}
	void Set(T val) {
		memcpy(ptr.get(), &val, sizeof(T));
	}

private:
	static const char *constructor_key;
	std::unique_ptr<data_t[]> ptr;
};

template <class T>
const char *PointerHolder<T>::constructor_key;

struct out_string_wrapper {
	const char *ptr;
};

class ValueConversion {
public:
	template <class T>
	static Napi::Value ToJS(Napi::Env &env, T val) {
		return ToJS_(env, val);
	}

private:
	template <class T>
	static Napi::Value ToJS_(Napi::Env &env, T val) {
		// static_assert(false, "Unimplemented value conversion to JS");
		throw "Unimplemented value conversion to JS";
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, duckdb_state val) {
		return Napi::Number::New(env, (uint8_t)val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, duckdb_result_type val) {
		return Napi::Number::New(env, (uint8_t)val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, duckdb_pending_state val) {
		return Napi::Number::New(env, (uint8_t)val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, duckdb_type val) {
		return Napi::Number::New(env, (uint8_t)val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, duckdb_data_chunk val) {
		return PointerHolder<duckdb_data_chunk>::NewAndSet(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, duckdb_query_progress_type val) {
		return PointerHolder<duckdb_query_progress_type>::NewAndSet(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, duckdb_value val) {
		return PointerHolder<duckdb_value>::NewAndSet(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, duckdb_vector val) {
		return PointerHolder<duckdb_vector>::NewAndSet(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, duckdb_logical_type val) {
		return PointerHolder<duckdb_logical_type>::NewAndSet(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, void *val) {
		return PointerHolder<void *>::NewAndSet(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, uint64_t *val) {
		return PointerHolder<uint64_t *>::NewAndSet(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, duckdb_string val) {
		auto ret = Napi::String::New(env, val.data, val.size);
		duckdb_free(val.data);
		return ret;
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, const char *val) {
		return Napi::String::New(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, char *val) {
		return Napi::String::New(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, int32_t val) {
		return Napi::Number::New(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, uint32_t val) {
		return Napi::Number::New(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, idx_t val) {
		return Napi::Number::New(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, bool val) {
		return Napi::Boolean::New(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, double val) {
		return Napi::Number::New(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, size_t val) {
		return Napi::Number::New(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, int8_t val) {
		return Napi::Number::New(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, int16_t val) {
		return Napi::Number::New(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, int64_t val) {
		return Napi::Number::New(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, uint8_t val) {
		return Napi::Number::New(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, uint16_t val) {
		return Napi::Number::New(env, val);
	}

	template <>
	Napi::Value ToJS_(Napi::Env &env, float val) {
		return Napi::Number::New(env, val);
	}

public:
	template <class T>
	static T FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return FromJS_<T>(info, offset);
	}

private:
	template <class T>
	static T FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		// static_assert(false, "Unimplemented value conversion from JS");
		throw "Unimplemented value conversion to JS";
	}

	template <>
	std::string FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::String>();
	}

	template <>
	duckdb_database *FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_database>::FromInfo(info, offset);
	}

	template <>
	duckdb_database FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS_<duckdb_database *>(info, offset);
	}

	template <>
	duckdb_query_progress_type *FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_query_progress_type>::FromInfo(info, offset);
	}

	template <>
	duckdb_query_progress_type FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS_<duckdb_query_progress_type *>(info, offset);
	}

	template <>
	duckdb_connection *FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_connection>::FromInfo(info, offset);
	}

	template <>
	duckdb_connection FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS_<duckdb_connection *>(info, offset);
	}

	template <>
	duckdb_config *FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_config>::FromInfo(info, offset);
	}

	template <>
	duckdb_config FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS_<duckdb_config *>(info, offset);
	}

	template <>
	duckdb_result *FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_result>::FromInfo(info, offset);
	}

	template <>
	duckdb_result FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS_<duckdb_result *>(info, offset);
	}

	template <>
	duckdb_prepared_statement *FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_prepared_statement>::FromInfo(info, offset);
	}

	template <>
	duckdb_prepared_statement FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS_<duckdb_prepared_statement *>(info, offset);
	}

	template <>
	duckdb_appender *FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_appender>::FromInfo(info, offset);
	}

	template <>
	duckdb_appender FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS_<duckdb_appender *>(info, offset);
	}

	template <>
	duckdb_data_chunk *FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_data_chunk>::FromInfo(info, offset);
	}

	template <>
	duckdb_data_chunk FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS_<duckdb_data_chunk *>(info, offset);
	}

	template <>
	duckdb_vector *FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_vector>::FromInfo(info, offset);
	}

	template <>
	duckdb_vector FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS_<duckdb_vector *>(info, offset);
	}

	template <>
	duckdb_pending_result *FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_pending_result>::FromInfo(info, offset);
	}

	template <>
	const char **FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return &(PointerHolder<out_string_wrapper>::FromInfo(info, offset)->ptr);
	}

	template <>
	char **FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return (char **)&(PointerHolder<out_string_wrapper>::FromInfo(info, offset)->ptr);
	}

	template <>
	duckdb_pending_result FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS_<duckdb_pending_result *>(info, offset);
	}

	template <>
	duckdb_pending_state FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return (duckdb_pending_state)GetValue(info, offset).As<Napi::Number>().Int32Value();
	}

	template <>
	duckdb_type FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return (duckdb_type)GetValue(info, offset).As<Napi::Number>().Int32Value();
	}

	template <>
	duckdb_logical_type FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS_<duckdb_logical_type *>(info, offset);
	}

	template <>
	duckdb_logical_type *FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_logical_type>::FromInfo(info, offset);
	}

	template <>
	duckdb_value FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS_<duckdb_value *>(info, offset);
	}

	template <>
	duckdb_value *FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_value>::FromInfo(info, offset);
	}

	template <>
	void *FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return *PointerHolder<void *>::FromInfo(info, offset);
	}

	template <>
	const void *FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return *PointerHolder<const void *>::FromInfo(info, offset);
	}

	template <>
	uint64_t *FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return *PointerHolder<uint64_t *>::FromInfo(info, offset);
	}

	template <>
	idx_t FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().Int64Value();
	}

	template <>
	size_t FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().Int64Value();
	}

	template <>
	int64_t FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().Int64Value();
	}

	template <>
	int32_t FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().Int32Value();
	}

	template <>
	uint32_t FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().Int32Value();
	}

	template <>
	int16_t FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().Int32Value();
	}

	template <>
	uint16_t FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().Int32Value();
	}

	template <>
	float FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().FloatValue();
	}

	template <>
	double FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().DoubleValue();
	}

	template <>
	int8_t FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().Int32Value();
	}

	template <>
	bool FromJS_(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Boolean>().Value();
	}
};
} // namespace duckdb_node