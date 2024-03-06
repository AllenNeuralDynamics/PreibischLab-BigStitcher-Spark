package net.preibisch.bigstitcher.spark.util;

import java.io.IOException;

import org.janelia.saalfeldlab.n5.N5FSWriter;
import org.janelia.saalfeldlab.n5.N5KeyValueWriter;
import org.janelia.saalfeldlab.n5.N5Writer;
import org.janelia.saalfeldlab.n5.hdf5.N5HDF5Writer;
import org.janelia.saalfeldlab.n5.s3.AmazonS3KeyValueAccess;
import org.janelia.saalfeldlab.n5.zarr.N5ZarrWriter;

import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.google.gson.GsonBuilder;

import net.preibisch.mvrecon.process.export.ExportN5API.StorageType;


public class N5Util
{
	// only supported for local spark HDF5 writes, needs to share a writer instance
	public static N5HDF5Writer hdf5DriverVolumeWriter = null;

	public static N5Writer createWriter(
			final String path,
			final StorageType storageType ) throws IOException // can be null if N5 or ZARR is written
	{
		if ( StorageType.N5.equals( storageType ) )
			return new N5FSWriter( path );
		else if ( StorageType.ZARR.equals( storageType ) )
			return new N5ZarrWriter( path );
		else if ( StorageType.HDF5.equals( storageType ) )
			return hdf5DriverVolumeWriter == null ? hdf5DriverVolumeWriter = new N5HDF5Writer( path ) : hdf5DriverVolumeWriter;
		else
			throw new RuntimeException( "storageType " + storageType + " not supported." );
	}


	public static N5Writer createWriter(
			final AmazonS3ClientBuilder s3ClientBuilder,
			final String s3Bucket,
			final String path,
			final StorageType storageType ) throws IOException // can be null if N5 or ZARR is written
	{
		if ( StorageType.N5.equals( storageType ) )
		{
			if ( s3Bucket == null )
			{
				return new N5FSWriter( path );
			}
			else
			{
				return new N5KeyValueWriter(
						new AmazonS3KeyValueAccess( s3ClientBuilder.build(), s3Bucket, false ), path, new GsonBuilder(), false );
			}
		}
		else if ( StorageType.ZARR.equals( storageType ) )
			return new N5ZarrWriter( path );
		else if ( StorageType.HDF5.equals( storageType ) )
			return hdf5DriverVolumeWriter == null ? hdf5DriverVolumeWriter = new N5HDF5Writer( path ) : hdf5DriverVolumeWriter;
		else
			throw new RuntimeException( "storageType " + storageType + " not supported." );

	}
}
