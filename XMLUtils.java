package com.lokesh.core.util;

import java.io.File;
import java.io.FileOutputStream;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;

import org.apache.log4j.Logger;
import org.apache.log4j.xml.DOMConfigurator;

/**
 * This class has APIs to help with XML tasks
 */
public class XMLUtils
{
    // Log4j logger object
    private static final Logger logger = Logger.getLogger( XMLUtils.class );

    static
    {
        // initialize log4j
        init();
    }

    private XMLUtils()
    {
    }

    /**
     * Convert a Java object into an XML File at the given path
     * @param classObj Java object that needs to to written into the XML file
     * @param xmlPath Path to the XML file where Java object needs to be written
     * @throws Exception
     */
    public static void marshall( Class<?> classObj, Object object, String xmlPath ) throws Exception
    {
        logger.debug( "Entering XMLUtils.marshall() with: [" + classObj + ", " + object + ", " + xmlPath + "]" );

        if ( xmlPath == null )
        {
            xmlPath = classObj.getSimpleName() + ".xml";
        }
        JAXBContext contextObj = JAXBContext.newInstance( classObj );
        Marshaller marshalleObj = contextObj.createMarshaller();

        marshalleObj.setProperty( Marshaller.JAXB_FORMATTED_OUTPUT, true );
        marshalleObj.marshal( object, new FileOutputStream( xmlPath ) );

        logger.debug( "Exiting XMLUtils.marshall() with: []" );
    }

    /**
     * Convert an XML File at the given path to a Java object
     * @param classObj Java Object which needs to be populated
     * @param xmlPath Path to the XML file path
     * @return Java object corresponding to the XML file
     */
    public static Object unMarshall( Class<?> classObj, String xmlPath ) throws Exception
    {
        logger.debug( "Entering XMLUtils.unMarshall() with: [" + classObj + ", " + xmlPath + "]" );

        if ( xmlPath == null )
        {
            xmlPath = classObj.getSimpleName() + ".xml";
        }

        JAXBContext contextObj = JAXBContext.newInstance( classObj );
        Unmarshaller marshalleObj = contextObj.createUnmarshaller();

        Object object = marshalleObj.unmarshal( new File( xmlPath ) );

        logger.debug( "Exiting XMLUtils.unMarshall() with: [" + object + "]" );
        return object;
    }

    /**
     * Initializes log4j configurations
     */
    private static void init()
    {
        DOMConfigurator.configure( "log4j.xml" );
    }
}
