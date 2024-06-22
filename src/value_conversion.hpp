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

static inline Napi::Value GetValue(const Napi::CallbackInfo &info, size_t offset) {
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
	template <class T, typename fake = void>
	static Napi::Value ToJS(Napi::Env &env, T val) {
		// static_assert(false, "Unimplemented value conversion to JS");
		throw "Unimplemented value conversion to JS";
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, duckdb_state val) {
		return Napi::Number::New(env, (uint8_t)val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, duckdb_result_type val) {
		return Napi::Number::New(env, (uint8_t)val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, duckdb_pending_state val) {
		return Napi::Number::New(env, (uint8_t)val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, duckdb_type val) {
		return Napi::Number::New(env, (uint8_t)val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, duckdb_data_chunk val) {
		return PointerHolder<duckdb_data_chunk>::NewAndSet(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, duckdb_query_progress_type val) {
		return PointerHolder<duckdb_query_progress_type>::NewAndSet(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, duckdb_value val) {
		return PointerHolder<duckdb_value>::NewAndSet(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, duckdb_vector val) {
		return PointerHolder<duckdb_vector>::NewAndSet(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, duckdb_logical_type val) {
		return PointerHolder<duckdb_logical_type>::NewAndSet(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, void *val) {
		return PointerHolder<void *>::NewAndSet(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, uint64_t *val) {
		return PointerHolder<uint64_t *>::NewAndSet(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, duckdb_string val) {
		auto ret = Napi::String::New(env, val.data, val.size);
		duckdb_free(val.data);
		return ret;
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, const char *val) {
		return Napi::String::New(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, char *val) {
		return Napi::String::New(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, int32_t val) {
		return Napi::Number::New(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, uint32_t val) {
		return Napi::Number::New(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, idx_t val) {
		return Napi::Number::New(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, bool val) {
		return Napi::Boolean::New(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, double val) {
		return Napi::Number::New(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, size_t val) {
		return Napi::Number::New(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, int8_t val) {
		return Napi::Number::New(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, int16_t val) {
		return Napi::Number::New(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, int64_t val) {
		return Napi::Number::New(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, uint8_t val) {
		return Napi::Number::New(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, uint16_t val) {
		return Napi::Number::New(env, val);
	}

	template <typename fake>
	Napi::Value ToJS(Napi::Env &env, float val) {
		return Napi::Number::New(env, val);
	}

	template <class T, typename fake = void>
	static T FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		// static_assert(false, "Unimplemented value conversion from JS");
		throw "Unimplemented value conversion to JS";
	}

	template <class T, typename fake>
	std::string FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::String>();
	}

	template <class T, typename fake>
	duckdb_database *FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_database>::FromInfo(info, offset);
	}

	template <class T, typename fake>
	duckdb_database FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS<duckdb_database *>(info, offset);
	}

	template <class T, typename fake>
	duckdb_query_progress_type *FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_query_progress_type>::FromInfo(info, offset);
	}

	template <class T, typename fake>
	duckdb_query_progress_type FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS<duckdb_query_progress_type *>(info, offset);
	}

	template <class T, typename fake>
	duckdb_connection *FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_connection>::FromInfo(info, offset);
	}

	template <class T, typename fake>
	duckdb_connection FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS<duckdb_connection *>(info, offset);
	}

	template <class T, typename fake>
	duckdb_config *FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_config>::FromInfo(info, offset);
	}

	template <class T, typename fake>
	duckdb_config FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS<duckdb_config *>(info, offset);
	}

	template <class T, typename fake>
	duckdb_result *FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_result>::FromInfo(info, offset);
	}

	template <class T, typename fake>
	duckdb_result FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS<duckdb_result *>(info, offset);
	}

	template <class T, typename fake>
	duckdb_prepared_statement *FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_prepared_statement>::FromInfo(info, offset);
	}

	template <class T, typename fake>
	duckdb_prepared_statement FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS<duckdb_prepared_statement *>(info, offset);
	}

	template <class T, typename fake>
	duckdb_appender *FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_appender>::FromInfo(info, offset);
	}

	template <class T, typename fake>
	duckdb_appender FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS<duckdb_appender *>(info, offset);
	}

	template <class T, typename fake>
	duckdb_data_chunk *FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_data_chunk>::FromInfo(info, offset);
	}

	template <class T, typename fake>
	duckdb_data_chunk FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS<duckdb_data_chunk *>(info, offset);
	}

	template <class T, typename fake>
	duckdb_vector *FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_vector>::FromInfo(info, offset);
	}

	template <class T, typename fake>
	duckdb_vector FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS<duckdb_vector *>(info, offset);
	}

	template <class T, typename fake>
	duckdb_pending_result *FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_pending_result>::FromInfo(info, offset);
	}

	template <class T, typename fake>
	const char **FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return &(PointerHolder<out_string_wrapper>::FromInfo(info, offset)->ptr);
	}

	template <class T, typename fake>
	char **FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return (char **)&(PointerHolder<out_string_wrapper>::FromInfo(info, offset)->ptr);
	}

	template <class T, typename fake>
	duckdb_pending_result FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS<duckdb_pending_result *>(info, offset);
	}

	template <class T, typename fake>
	duckdb_pending_state FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return (duckdb_pending_state)GetValue(info, offset).As<Napi::Number>().Int32Value();
	}

	template <class T, typename fake>
	duckdb_type FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return (duckdb_type)GetValue(info, offset).As<Napi::Number>().Int32Value();
	}

	template <class T, typename fake>
	duckdb_logical_type FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS<duckdb_logical_type *>(info, offset);
	}

	template <class T, typename fake>
	duckdb_logical_type *FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_logical_type>::FromInfo(info, offset);
	}

	template <class T, typename fake>
	duckdb_value FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return *FromJS<duckdb_value *>(info, offset);
	}

	template <class T, typename fake>
	duckdb_value *FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return PointerHolder<duckdb_value>::FromInfo(info, offset);
	}

	template <class T, typename fake>
	void *FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return *PointerHolder<void *>::FromInfo(info, offset);
	}

	template <class T, typename fake>
	const void *FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return *PointerHolder<const void *>::FromInfo(info, offset);
	}

	template <class T, typename fake>
	uint64_t *FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return *PointerHolder<uint64_t *>::FromInfo(info, offset);
	}

	template <class T, typename fake>
	idx_t FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().Int64Value();
	}

	template <class T, typename fake>
	size_t FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().Int64Value();
	}

	template <class T, typename fake>
	int64_t FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().Int64Value();
	}

	template <class T, typename fake>
	int32_t FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().Int32Value();
	}

	template <class T, typename fake>
	uint32_t FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().Int32Value();
	}

	template <class T, typename fake>
	int16_t FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().Int32Value();
	}

	template <class T, typename fake>
	uint16_t FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().Int32Value();
	}

	template <class T, typename fake>
	float FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().FloatValue();
	}

	template <class T, typename fake>
	double FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().DoubleValue();
	}

	template <class T, typename fake>
	int8_t FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Number>().Int32Value();
	}

	template <class T, typename fake>
	bool FromJS(const Napi::CallbackInfo &info, idx_t offset) {
		return GetValue(info, offset).As<Napi::Boolean>().Value();
	}
};
} // namespace duckdb_node