/* seekdb <-> hipVS bridge: plain-C CAGRA kNN; all cuVS/ROCm deps live here. */
#include "seekdb_cuvs_bridge.h"
#include <cuvs/core/c_api.h>
#include <cuvs/neighbors/cagra.h>
#include <dlpack/dlpack.h>
#ifdef __HIP_PLATFORM_AMD__
#include <cuvs/cuda_runtime.h>
#else
#include <cuda_runtime.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <pthread.h>

typedef struct {
  cuvsResources_t        res;
  cuvsCagraIndexParams_t ip;
  cuvsCagraIndex_t       idx;
  cuvsCagraSearchParams_t sp;
  size_t default_itopk;
  void *workspace;
  size_t workspace_bytes;
  pthread_mutex_t search_mutex;
  long n, dim;
} sk_cuvs_index;

static sk_cuvs_index* create_handle(void)
{
  sk_cuvs_index* h = (sk_cuvs_index*)calloc(1, sizeof(*h));
  if (!h) return NULL;
  if (pthread_mutex_init(&h->search_mutex, NULL) != 0) { free(h); return NULL; }
  if (cuvsResourcesCreate(&h->res) != CUVS_SUCCESS) {
    pthread_mutex_destroy(&h->search_mutex);
    free(h);
    return NULL;
  }
  return h;
}

/* Keep one bounded scratch allocation per live index. A failed growth leaves
 * the handle reusable, and releases the old allocation before asking for more. */
static int reserve_workspace(sk_cuvs_index* h, size_t bytes)
{
  if (h->workspace_bytes >= bytes) return 0;
  if (h->workspace) {
    if (cuvsRMMFree(h->res, h->workspace, h->workspace_bytes) != CUVS_SUCCESS) return 1;
    h->workspace = NULL;
    h->workspace_bytes = 0;
  }
  void* buffer = NULL;
  if (cuvsRMMAlloc(h->res, &buffer, bytes) != CUVS_SUCCESS) return 1;
  h->workspace = buffer;
  h->workspace_bytes = bytes;
  return 0;
}

void* seekdb_cuvs_build(const float* base, long n, long dim)
{
  if (!base || n <= 0 || dim <= 0) return NULL;
  sk_cuvs_index* h = create_handle();
  if (!h) return NULL;

  DLManagedTensor dset; long ds[2] = {n, dim};
  dset.dl_tensor.data = (void*)base;
  dset.dl_tensor.device.device_type = kDLCPU; dset.dl_tensor.device.device_id = 0;
  dset.dl_tensor.ndim = 2;
  dset.dl_tensor.dtype.code = kDLFloat; dset.dl_tensor.dtype.bits = 32; dset.dl_tensor.dtype.lanes = 1;
  dset.dl_tensor.shape = ds; dset.dl_tensor.strides = 0; dset.dl_tensor.byte_offset = 0;

  if (cuvsCagraIndexParamsCreate(&h->ip) != CUVS_SUCCESS) goto fail_res;
  h->ip->build_algo = NN_DESCENT;
  h->ip->graph_degree = 128;
  h->ip->intermediate_graph_degree = 256;
  h->ip->nn_descent_niter = 100;
  if (cuvsCagraIndexCreate(&h->idx)      != CUVS_SUCCESS) goto fail_ip;
  if (cuvsCagraBuild(h->res, h->ip, &dset, h->idx) != CUVS_SUCCESS) goto fail_idx;
  h->n = n; h->dim = dim;
  return h;

fail_idx: cuvsCagraIndexDestroy(h->idx);
fail_ip:  cuvsCagraIndexParamsDestroy(h->ip);
fail_res: cuvsResourcesDestroy(h->res); pthread_mutex_destroy(&h->search_mutex); free(h);
  fprintf(stderr, "[seekdb_cuvs] build failed: %s\n", cuvsGetLastErrorText());
  return NULL;
}

int seekdb_cuvs_search(void* handle, const float* query, long nq, long topk,
                       unsigned int* out_ids, float* out_dist)
{
  sk_cuvs_index* h = (sk_cuvs_index*)handle;
  if (!h || !query || !out_ids || nq <= 0 || topk <= 0 || topk > h->n) return 1;
  if ((size_t)nq > SIZE_MAX / sizeof(float) / (size_t)h->dim ||
      (size_t)nq > SIZE_MAX / (sizeof(unsigned) + sizeof(float)) / (size_t)topk ||
      (size_t)topk > SIZE_MAX / 4) return 1;
  const size_t query_bytes = sizeof(float) * (size_t)nq * (size_t)h->dim;
  const size_t id_bytes = sizeof(unsigned) * (size_t)nq * (size_t)topk;
  const size_t distance_bytes = sizeof(float) * (size_t)nq * (size_t)topk;
  if (query_bytes > SIZE_MAX - id_bytes - distance_bytes) return 1;
  if (pthread_mutex_lock(&h->search_mutex) != 0) return 1;
  int rc = 0;
  if (reserve_workspace(h, query_bytes + id_bytes + distance_bytes)) { rc=10; goto done; }
  if (!h->sp) {
    if (cuvsCagraSearchParamsCreate(&h->sp) != CUVS_SUCCESS) { rc=14; goto done; }
    h->default_itopk = h->sp->itopk_size;
  }
  {
    float* qd = (float*)h->workspace;
    unsigned* nd = (unsigned*)((char*)h->workspace + query_bytes);
    float* dd = (float*)((char*)nd + id_bytes);
    if (cudaMemcpy(qd, query, query_bytes, cudaMemcpyDefault) != cudaSuccess) { rc=13; goto done; }
    DLManagedTensor qt, nt, dt; long qs[2]={nq,h->dim}, nsh[2]={nq,topk}, dsh[2]={nq,topk};
    qt.dl_tensor.data=qd; qt.dl_tensor.device.device_type=kDLCUDA; qt.dl_tensor.device.device_id=0;
    qt.dl_tensor.ndim=2; qt.dl_tensor.dtype.code=kDLFloat; qt.dl_tensor.dtype.bits=32; qt.dl_tensor.dtype.lanes=1;
    qt.dl_tensor.shape=qs; qt.dl_tensor.strides=0; qt.dl_tensor.byte_offset=0;
    nt=qt; nt.dl_tensor.data=nd; nt.dl_tensor.dtype.code=kDLUInt; nt.dl_tensor.shape=nsh;
    dt=qt; dt.dl_tensor.data=dd; dt.dl_tensor.dtype.code=kDLFloat; dt.dl_tensor.shape=dsh;
    {
      size_t required_itopk = (size_t)topk * 4;
      if (required_itopk < 512) required_itopk = 512;
      h->sp->itopk_size = h->default_itopk > required_itopk ? h->default_itopk : required_itopk;
      h->sp->search_width = 4;
    }
    cuvsFilter f; f.type=NO_FILTER; f.addr=(uintptr_t)0;
    if (cuvsCagraSearch(h->res, h->sp, h->idx, &qt, &nt, &dt, f) != CUVS_SUCCESS) { rc=15; goto done; }
    if (cudaMemcpy(out_ids, nd, id_bytes, cudaMemcpyDefault) != cudaSuccess) { rc=16; goto done; }
    if (out_dist && cudaMemcpy(out_dist, dd, distance_bytes, cudaMemcpyDefault) != cudaSuccess) { rc=17; goto done; }
  }
done:
  pthread_mutex_unlock(&h->search_mutex);
  if (rc) fprintf(stderr, "[seekdb_cuvs] search rc=%d: %s\n", rc, cuvsGetLastErrorText());
  return rc;
}

void seekdb_cuvs_free(void* handle)
{
  sk_cuvs_index* h = (sk_cuvs_index*)handle;
  if (!h) return;
  if (h->sp) cuvsCagraSearchParamsDestroy(h->sp);
  if (h->workspace) cuvsRMMFree(h->res, h->workspace, h->workspace_bytes);
  cuvsCagraIndexDestroy(h->idx);
  cuvsCagraIndexParamsDestroy(h->ip);
  cuvsResourcesDestroy(h->res);
  pthread_mutex_destroy(&h->search_mutex);
  free(h);
}

int seekdb_cuvs_serialize(void* handle, const char* filename)
{
  sk_cuvs_index* index = (sk_cuvs_index*)handle;
  if (!index || !filename) return 1;
  if (cuvsCagraSerialize(index->res, filename, index->idx, true) != CUVS_SUCCESS) {
    fprintf(stderr, "[seekdb_cuvs] serialize failed: %s\n", cuvsGetLastErrorText());
    return 1;
  }
  return 0;
}

void* seekdb_cuvs_deserialize(const char* filename, long n, long dim)
{
  if (!filename || n <= 0 || dim <= 0) return NULL;
  sk_cuvs_index* index = create_handle();
  if (!index) return NULL;
  if (cuvsCagraIndexParamsCreate(&index->ip) != CUVS_SUCCESS) goto fail_res;
  if (cuvsCagraIndexCreate(&index->idx) != CUVS_SUCCESS) goto fail_ip;
  if (cuvsCagraDeserialize(index->res, filename, index->idx) != CUVS_SUCCESS) goto fail_idx;
  {
    int64_t actual_n = 0;
    int64_t actual_dim = 0;
    if (cuvsCagraIndexGetSize(index->idx, &actual_n) != CUVS_SUCCESS ||
        cuvsCagraIndexGetDims(index->idx, &actual_dim) != CUVS_SUCCESS ||
        actual_n != n || actual_dim != dim) {
      fprintf(stderr, "[seekdb_cuvs] snapshot shape mismatch: %ld x %ld, expected %ld x %ld\n",
              (long)actual_n, (long)actual_dim, n, dim);
      goto fail_idx;
    }
  }
  index->n = n;
  index->dim = dim;
  return index;
fail_idx:
  cuvsCagraIndexDestroy(index->idx);
fail_ip:
  cuvsCagraIndexParamsDestroy(index->ip);
fail_res:
  cuvsResourcesDestroy(index->res);
  pthread_mutex_destroy(&index->search_mutex);
  fprintf(stderr, "[seekdb_cuvs] deserialize failed: %s\n", cuvsGetLastErrorText());
  free(index);
  return NULL;
}

int seekdb_cuvs_cagra_knn(const float* base, long n, long dim,
                          const float* query, long nq, long topk,
                          unsigned int* out_ids)
{
  void* h = seekdb_cuvs_build(base, n, dim);
  if (!h) return 10;
  int rc = seekdb_cuvs_search(h, query, nq, topk, out_ids, NULL);
  seekdb_cuvs_free(h);
  return rc;
}
